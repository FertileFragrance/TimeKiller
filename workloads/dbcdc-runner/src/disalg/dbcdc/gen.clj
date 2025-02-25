(ns disalg.dbcdc.gen
  (:require[jepsen.generator :as gen]))

(defrecord LimitOK [remaining gen]
  gen/Generator
  (op [_ test ctx]
    (when (pos? remaining)
      (when-let [[op gen'] (gen/op gen test ctx)]
        [op (LimitOK. remaining gen')])))

  (update [this test ctx event]
          (if (= :ok (:type event))
            (LimitOK. (dec remaining) (gen/update gen test ctx event))
            (LimitOK. remaining (gen/update gen test ctx event)))))

(defn limit-ok
  "Wraps a generator and ensures that it returns at most `limit` operations.
  Propagates every update to the underlying generator."
  [remaining gen]
  (LimitOK. remaining gen))