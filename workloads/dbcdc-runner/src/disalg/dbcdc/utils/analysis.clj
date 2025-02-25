(ns disalg.dbcdc.utils.analysis
  (:require [disalg.dbcdc.utils.cfg :refer [read-history]]
            [elle.rw-register :as rw]
            [clojure.java.io :as io]))

(def filepath "./store/latest/history.edn")
(def h (read-history filepath))

(defn check-history
  [h]
  (rw/check {:consistency-models [:snapshot-isolation]} h))

(defn list-directories [dir]
  (let [files (map #(.getCanonicalFile %) (.listFiles (io/file dir)))]
    (concat
      (filter #(.isDirectory %) files)
      (apply concat (map #(list-directories %) (filter #(.isDirectory %) files))))))

(def store-path "./store/")

(defn list-files [dir]
  (let [files (map #(.getCanonicalFile %) (.listFiles (io/file dir)))]
    (concat
      (filter #(.isFile %) files)
      (apply concat (map #(list-files %) (filter #(.isDirectory %) files))))))

(defn check-instance-file
  [instance-file]
  (let [history-file (io/file instance-file "history.edn")
        history-path (.getAbsolutePath history-file)]
    (let [start-time (System/currentTimeMillis)
          execute    (check-history (read-history history-path))
          end-time   (System/currentTimeMillis)]
      (- end-time start-time))))

(defn average [coll] 
  (/ (reduce + coll) (count coll)))

(defn check-param-file
  [param-file]
  (let [instances (.listFiles param-file) 
        instances (filter #(not (re-find #"latest" (.getName %))) instances)]
    {:param (.getName param-file) :time (average (map check-instance-file instances))}))

(defn calculate-time
  [store-path]
  (let [files (.listFiles (io/file store-path))
        files (filter #(re-find #"dbcdc" (.getName %)) files)]
    (map check-param-file files)))
