(ns disalg.dbcdc.rw
  "Test for transactional read-write register write"
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
             [string :as str]]
            [dom-top.core :refer [with-retry]]
            [elle.core :as elle]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]
             [util :as util]
             [store :as store]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.tests.cycle.wr :as wr]
            [elle.rw-register :as r]
            [disalg.dbcdc [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]
            [disalg.dbcdc.utils.loader :as loader]
            [disalg.dbcdc.utils.generator :as dbcop-gen]
            [disalg.dbcdc.impls.dgraph :as dgraph]
            [disalg.dbcdc.impls.mongo :as mongo]
            [disalg.dbcdc.client :as c]
            [jepsen.checker :as checker]
            [clojure.edn :as edn]))

(def default-table-count 1)

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn mop!
  "Executes a transactional micro-op on a connection. 
   Returns the completed micro-op."
  [conn test [f k v]]
  (let [table-count (:table-count test default-table-count)
        table (table-for table-count k)]
    (Thread/sleep (rand-int 10))
    [f k (case f
           :r (c/read conn table k test)
           :w (let [_ (c/write conn table k v test)]
                v))]))

(defn execute-txn-retional
  [test conn txn isolation]
  (j/with-transaction [t conn
                       {:isolation isolation}]
    (let [txn' (mapv (partial mop! t test) txn)]
      {:type :ok :value txn'})))

(defn execute-txn-nosql
  [test conn txn isolation]
  (let [database (:database test)]
    (case database
      (case database
        :dgraph  (let [session-id conn
                       spec       (c/get-spec test)]
                   (dgraph/execute-txn-v2 session-id spec txn))
        :mongodb (let [{:keys [conn session]} conn
                       spec   (c/get-spec test)]
                   (mongo/execute-txn conn session txn spec test))))))

(defn retry-func
  [test conn txn isolation]
  (try
    (let [database (:database test)]
      (if (c/relation-db? database)
        (execute-txn-retional test conn txn isolation)
        (execute-txn-nosql test conn txn isolation)))
    (catch Exception e
      (warn "retry" txn "with" (.getMessage e))
      nil)))

(defn retry?
  [ret]
  (or
   (nil? ret)
   (not (map? ret))
   (not= :ok (:type ret))))

(defn execute-txn
  [test conn txn isolation]
  (let [ret (atom nil)
        idx (atom 0)]
    (while (retry? @ret)
      (let [_ (swap! idx inc)
            _ (info "retry" txn "with ret" @ret)]
        (reset! ret (retry-func test conn txn isolation))))
    @ret))

; initialized? is an atom which we set when we first use the connection--we set
; up initial isolation levels, logging info, etc. This has to be stateful
; because we don't necessarily know what process is going to use the connection
; at open! time.
(defrecord Client [node conn initialized? variables]
  client/Client
  (open! [this test node]
    (let [c (c/open test node)]
      (assoc this
             :node          node
             :conn          c
             :initialized?  (atom false)
             :variables     variables)))

  (setup! [_ test]
    (info "Client setup!")
    (dotimes [i (:table-count test default-table-count)]
      (with-retry [conn  conn
                   tries 10]
        (c/create-table conn (table-name i) test)
        (catch org.postgresql.util.PSQLException e
          (condp re-find (.getMessage e)
            #"duplicate key value violates unique constraint"
            :dup

            #"An I/O error occurred|connection has been closed"
            (do (when (zero? tries)
                  (throw e))
                (info "Retrying IO error")
                (Thread/sleep 1000)
                (c/close! test conn)
                (retry (c/await-open test node)
                       (dec tries)))

            (throw e))))))

  (invoke! [_ test op]
    ; One-time connection setup
    (when (compare-and-set! initialized? false true)
      ; for postgresql only
      (when (= (:database test) :postgresql)
        (j/execute! conn [(str "set application_name = 'jepsen process " (:process op) "'")]))
      (c/set-transaction-isolation! conn (:isolation test) test))
    (c/with-errors op
      (let [isolation (c/isolation-mapping (:isolation test) test)
            txn       (:value op)
            txn'      (execute-txn test conn txn isolation)]
        (merge op txn'))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! test conn)))

(defn rw-test
  [opts]
  {:generator (wr/gen opts)
   :checker   (wr/checker opts)})

(defn elle-rw-workload
  [opts]
  (-> (rw-test (assoc (select-keys opts [:key-count
                                         :max-txn-length
                                         :max-writes-per-key])
                      :min-txn-length 1
                      :consistency-models [(:expected-consistency-model opts)]))
      (assoc :client (Client. nil nil nil nil))))

(defn save-only-ok-invoke
  [history]
  (let [_ (info "save-only-ok-invoke")]
    (filterv (fn [op]
               (or (= :ok (:type op)) (= :invoke (:type op))))
             history)))

(defn non-fail-checker
  [opts]
  (reify checker/Checker
    (check [this test history opts]
      (let [ok-history (save-only-ok-invoke history)
            _          (info "checking history" ok-history)]
        (r/check (assoc opts :directory
                        (.getCanonicalPath
                         (store/path! test (:subdirectory opts) "elle")))
                 ok-history)))))

(defn dbcop-rw-workload
  [opts]
  (let [workload-path (:dbcop-workload-path opts) ;; get path of history.json
        testcase      (loader/load-testcase workload-path) ;; after history.json loaded
        {:keys [_ variables txns]} testcase]
    (-> {:generator (dbcop-gen/dbcop-generator opts txns)
         :checker   (wr/checker {:consistency-models [(:expected-consistency-model opts)]})}
        (assoc :client (Client. nil nil nil variables)))))

(defn workload
  "A read-write register write workload."
  [opts]
  (let [is-dbcop (:dbcop-workload opts)]
    (if is-dbcop
      (dbcop-rw-workload opts)
      (elle-rw-workload opts))))