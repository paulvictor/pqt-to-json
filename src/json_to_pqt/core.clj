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

(defn select-only-valid-keys
  [valid-keys k v]
  (and
   (valid-keys k)
   (not (vector? v))
   v))

(defn -main [& args]
  (let [{:keys [output-schema]} (config/load-config)
        {{:keys [input-file output-file]} :options} (parse-opts args cli-opts)]
    (prn output-schema)
    (with-open [input
                (-> input-file file FileInputStream.)]
      (let
          [valid-keys (let [ vkeys (hash-set (keys output-schema))]

                        (prn vkeys)
                        vkeys)
           stream (->> input
                       GZIPInputStream.
                       java.io.InputStreamReader.
                       java.io.BufferedReader.
                       line-seq
                       (map #(json/read-str %
                                            :key-fn keyword
                                            :value-fn (partial select-only-valid-keys valid-keys)))
                       (take 100000)
                       ;; (map (fn [j]
;;                               (prn j)
;;                               j))

                       (partition-all 10000)
                       (map #(ds/->dataset % {:parser-fn output-schema})))
           parquet-options {:compression-codec :zstd}]
        (parquet/ds-seq->parquet  output-file parquet-options stream)))))
