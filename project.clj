(defproject hello "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [ring/ring-core "1.4.0"]
                 ;; [ring/ring-jetty-adapter "1.4.0"]
                 [http-kit "2.1.19"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [org.postgresql/postgresql "9.4-1201-jdbc41"]
                 [cheshire "5.5.0"]
                 [compojure "1.4.0"]
                 [ring/ring-defaults "0.1.5"]
                 [com.taoensso/carmine "2.11.1"]
                 [prismatic/schema "0.4.4"]]
  ;; :plugins [[lein-ring "0.8.11"]]
  :ring {:handler hello.core}
  :main hello.core)
