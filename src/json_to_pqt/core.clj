(ns json_to_pqt.core
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io :refer [input-stream output-stream file]]
   [clojure.data.json :as json]
   [cprop.core :as config :refer [load-config]]
   [cli-matic.core :refer [run-cmd]]
   [clojure.tools.cli :refer [parse-opts]]
   [tech.v3.libs.parquet :as parquet]
   [tech.v3.dataset :as ds])
  (:import
   (java.util.zip GZIPInputStream
                  GZIPOutputStream)
   (java.io FileInputStream
            FileOutputStream
            BufferedInputStream
            ByteArrayOutputStream)
   (java.time.format DateTimeParseException)
   (java.time Instant))
  (:gen-class))

(def cli-opts
  [["-i" "--input-file FILE"]
   ["-o" "--output-file FILE"]])

(defn conform-to-schema
  [schema k v]
  (when-let [desired-type (schema k)]
    (cond
      (= 'DateTime desired-type)
      (try
        (Instant/parse v)
        (catch DateTimeParseException e nil))
      (and (= desired-type Long)
           (instance? Double v))
      (Math/round v)
      :else v)))

;; TODO instead of filtering null keys, we should typecast the nil's to the respective types as mentioned in the schema
(defn filter-null-keys
  [m]
  (into {}
        (filter
         (fn [[k v]]
           (not (nil? v)))
         m)))

(defn -main [& args]
  (let [{:keys [output-schema]} (config/load-config)
        {{:keys [input-file output-file]} :options} (parse-opts args cli-opts)]
    (with-open [input
                (-> input-file file FileInputStream.)]
      (let
          [stream (->> input
                       GZIPInputStream.
                       java.io.InputStreamReader.
                       java.io.BufferedReader.
                       line-seq
                       (map #(json/read-str %
                                            :key-fn keyword
                                            :value-fn (partial conform-to-schema output-schema)))
                       (map filter-null-keys)
                       (partition-all 10000)
                       (map ds/->dataset))
           parquet-options {:compression-codec :zstd}]
        (parquet/ds-seq->parquet  output-file parquet-options stream)))))
