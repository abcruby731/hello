(ns hello.core
  (:require [org.httpkit.server :refer [run-server]]
            [ring.util.response :as rur]
            ;; [ring.middleware.resource :as rmr]
            ;; [ring.middleware.file :refer [wrap-file]]
            ;; [ring.middleware.content-type :refer [wrap-content-type]]
            ;; [ring.middleware.not-modified :refer [wrap-not-modified]]
            ;; [ring.middleware.cookies :refer [wrap-cookies]]
            ;; [ring.middleware.session :refer [wrap-session]]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :refer [generate-string parse-string]]
            ;; [ring.middleware.params :refer [wrap-params]]
            ;; [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [compojure.route :as route]
            [compojure.core :as core]
            [taoensso.carmine :as car :refer (wcar)]
            [schema.core :as s
             :include-macros true]))

(def server1-conn {:pool {} :spec {}})
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

(let [db-host "localhost"
      db-port 5432
      db-name "mydb"]
  (def db-spec {:subprotocol "postgresql"
                :subname (str "//" db-host ":" db-port "/" db-name)
                :user "aaa"
                :password "123"}))

(def Positive (s/pred pos?))

(def Data
  "A schema for a nested data type"
  {(s/required-key "id") s/Int
   (s/required-key "name") s/Str
   (s/required-key "author") s/Str
   (s/required-key "price") Positive})


(defn redis-works [id]
  (str "works:" (Integer/parseInt id)))

;; (defn to-map [body]
;;   (parse-string (slurp body)))

(defn query-from-db [id]
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/query conn ["select * from works where id = ?" (Integer/parseInt id)])))

(defn delete-from-db [id]
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/delete! conn :works ["id = ?" (Integer/parseInt id)])))

(defn update-db [id body]
  ;; (prn "Debug: " {:body (to-map body)})
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/update! conn :works body ["id = ?" (Integer/parseInt id)])))

(defn insert-db [body]
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/insert! conn :works body)))

(core/defroutes my-routes
  (core/GET "/works/:id" [id :as {uri :uri}]
    (if (nil? (s/check s/Int (Integer/parseInt (str (last uri)))))
      (let [require-work (redis-works id)]
        (if (zero? (wcar* (car/exists require-work)))
          (let [work-map (first (query-from-db id))]
            (if (nil? work-map)
              {:status 404
               :body "404 not found!"}
              (do
                (wcar* (apply car/hmset require-work (reduce into work-map))
                       (car/expire require-work 7200))
                {:status 200
                 :headers {"Content-Type" "application/json"}
                 :body (-> (wcar* (car/hgetall require-work))
                           generate-string)})))
          {:status 200
           :headers {"Content-Type" "application/json"}
           :body (-> (wcar* (car/hgetall require-work))
                     generate-string)}))
      {:status 400
       :body "400 bad request!"}
      )
    )
  (core/DELETE "/works/:id" [id]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (do (wcar* (car/del (redis-works id)))
               (delete-from-db id)
               "ok")
     })
  (core/PATCH "/works/:id" [id :as {body :body}]
              (let [body (parse-string (slurp body))]
                (prn id)
                (if (nil? (s/check Data body))
                  (let [require-work (redis-works id)
                        work-map (first (query-from-db id))]
                    (if (nil? work-map)
                      {:status 404
                       :body "404 not found"}
                      (do
                        (update-db id body)
                        (wcar* (apply car/hmset require-work (reduce into body))
                               (car/expire require-work 7200))
                        {:status 200
                         :body "ok"})))
                  {:status 400
                   :body "400 bad request"})))
  (core/POST "/works" [:as {body :body}]

    (let [body (parse-string (slurp body))
          require-work (redis-works (str (body "id")))
          ]
      (prn body)
      (if (nil? (s/check Data body))
        (do
          (insert-db body)
          (prn (wcar* (apply car/hmset require-work (reduce into body))
                      (car/expire require-work 7200)))
          {:status 200
           :headers {"Content-Type" "application/json"}
           :body (generate-string body)})
        {:status 400
         :body "400 bad request"})
      )
    ;; {:status 200
    ;;  :headers {"Content-Type" "text/plain"}
    ;;  :body (let [body (parse-string (slurp body))]
    ;;          (do (insert-db body)
    ;;              (wcar* (car/set (redis-works (str (body "id"))) body)
    ;;                     (car/expire (redis-works (str (body "id"))) 3600))
    ;;              (str "make done!")))
    ;;  }
    )
  (route/not-found "404 not found"))

;; (core/defroutes my-routes
;;   (core/context "" [id :as {uri :uri}]
;;     (core/GET "/works/:id" []
;;       {:status 200
;;        :headers {"Content-Type" "application/json"}
;;        :body (-> (jdbc/with-db-connection [conn db-spec]
;;                    (jdbc/query conn ["select * from works where id = ?"
;;                                      (Integer/parseInt (str (last uri)))])))})
;;     (core/DELETE "/works/:id" []
;;       (do (jdbc/with-db-connection [conn db-spec]
;;             (jdbc/delete! conn :works ["id = ?" (Integer/parseInt (str (last uri)))]))
;;           {:status 200
;;            :headers {"Content-Type" "text/plain"}
;;            :body (str "work_id = " id " deleted!")}))
;;     ))



;; (defn handler [request]
;;   (let [{:keys [request-method uri params]} request]
;;     (case request-method
;;       :get {:status 200
;;             :headers {"Content-Type" "application/json"}
;;             :body (-> (jdbc/with-db-connection [conn db-spec]
;;                         (jdbc/query conn ["select * from works where id = ?" (Integer/parseInt (str (last uri)))]))
;;                       first
;;                       generate-string)}
;;       :post {:status 200
;;              :headers {"Content-Type" "text/plain"}
;;              :body (first (jdbc/with-db-connection [conn db-spec]
;;                             (jdbc/insert! conn :works
;;                                           (parse-string (slurp (:body request)) true))))}
;;       :patch (do
;;                (jdbc/with-db-connection [conn db-spec]
;;                  (jdbc/update! conn :works {:price (Integer/parseInt (params "price"))} ["id = ?" (Integer/parseInt (str (last uri)))]))

;;                {:status 200
;;                :headers {"Content-Type" "text/plain"}
;;                 :body "ok"})
;;       :delete (do
;;                 (jdbc/with-db-connection [conn db-spec]
;;                   (jdbc/delete! conn :works ["id = ?" (Integer/parseInt (str (last uri)))]))
;;                 {:status 200
;;                  :headers {"Content-Type" "text/plain"}
;;                  :body "delete!"})
;;       {:status 200
;;        :header {"Content-Type" "text/plain"}
;;        :body ("hello world")})))


;;(defn wrap-content-type [handler content-type]
;;(fn [request]
;;(let [response (handler request)]
;;(assoc-in response [:header "Content-Type"] content-type))))

;; (defn wrap-prn-request [handler]
;;   (fn [request]
;;     (let [response (handler request)]
;;       (println "Debug: " (clojure.pprint/pprint request))
;;       response)))

(def app my-routes)

(defonce server (atom nil))

(defn stop-server []
  (when-not (nil? @server)
    ;; graceful shutdown: wait 100ms for existing requests to be finished
    ;; :timeout is optional, when no timeout, stop immediately
    (@server :timeout 100)
    (reset! server nil)))

(defn -main [& args]
  ;; The #' is useful when you want to hot-reload code
  ;; You may want to take a look: https://github.com/clojure/tools.namespace
  ;; and http://http-kit.org/migration.html#reload
  (reset! server (run-server #'app {:port 3000}))
  (prn "Server started!"))
