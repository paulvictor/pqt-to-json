(ns json_to_pqt.core
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io :refer [input-stream output-stream file]]
   [clojure.data.json :as json]
   [cprop.core :as config :refer [load-config]]
   [cli-matic.core :refer [run-cmd]]
   [clojure.tools.cli :refer [parse-opts]]
   [tech.v3.libs.parquet :as parquet])
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

(defn from-gzip
  [input]
  (-> input
      GZIPInputStream.))

(def cli-opts
  [["-i" "--input-file FILE"
    "-o" "--output-file FILE"]])

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
      (println (str "Parsing for" output-schema))
      (let
          [stream (->> input
                       GZIPInputStream.
                       java.io.InputStreamReader.
                       java.io.BufferedReader.
                       line-seq
                       (map #(json/read-str %
                                            :key-fn keyword
                                            :value-fn (partial conform-to-schema output-schema)))
                       )
           parquet-options {:compression-codec :zstd}]
        (parquet/ds-seq->parquet output-file (map filter-null-keys stream))))))
