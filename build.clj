(ns build
  (:require [clojure.tools.build.api :as b]))

(def build-folder "target")
(def jar-content (str build-folder "/classes"))


(def version "0.0.1")
(def app-name "json-to-pqt")

(def uber-file-name (format "%s/%s-%s-standalone.jar" build-folder app-name version))

(def basis (b/create-basis {:project "deps.edn"}))

(defn clean [_]
  (b/delete {:path build-folder})
  (println (format "Build folder \"%s\" removed" build-folder)))


(defn uber [_]
                                        ;clean leftovers
  (clean nil)
                                        ;copy sources and resources
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir jar-content})

  (b/compile-clj {:basis basis
                  :src-dirs ["src"]
                  :class-dir jar-content})

                                        ;pack a jar
  (b/uber {:class-dir jar-content
           :uber-file uber-file-name
           :basis basis
           :main 'json_to_pqt.core})
  (println (format "Jar created \"%s\"" uber-file-name)))
