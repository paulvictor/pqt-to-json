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
   (java.io FileInputStream))
  (:gen-class))

(def cli-opts
  [["-i" "--input-file FILE"]
   ["-o" "--output-file FILE"]])

(defn select-only-valid-keys
  [valid-keys k v]
  (when (and
         (valid-keys k)
         (not (vector? v)))
    v))

(defn -main [& args]
  (let [{:keys [output-schema]} (config/load-config)
        {{:keys [input-file output-file]} :options} (parse-opts args cli-opts)]
    (with-open [input
                (-> input-file file FileInputStream.)]
      (let
          [valid-keys (into (hash-set) (keys output-schema))
           stream (->> input
                       GZIPInputStream.
                       java.io.InputStreamReader.
                       java.io.BufferedReader.
                       line-seq
                       (map #(json/read-str %
                                            :key-fn keyword
                                            :value-fn (partial select-only-valid-keys valid-keys)))
                       (take 194242)
                       (partition-all 194242)
                       (map #(ds/->dataset % {:parser-fn output-schema})))
           parquet-options {:compression-codec :zstd}]
        (parquet/ds-seq->parquet  output-file parquet-options stream)))))
