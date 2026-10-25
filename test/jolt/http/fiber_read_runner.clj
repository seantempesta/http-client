(ns jolt.http.fiber-read-runner
  "A read from a fiber parks the fiber, not its carrier thread. On a main-carrier process
  (jolt.host/main-carrier-run!: one thread runs every fiber, as a forking program's agents do) a
  read that held the thread kept a server fiber in the same process from ever answering it: the
  read waited for good. Its own process, since main-carrier-run! must come before the first
  fiber. A watchdog thread fails a read that never returns instead of hanging."
  (:require [jolt.http.net :as net] [clojure.core.async :as a]))

(defn -main [& _]
  (let [outcome (promise)]
    (.start (Thread. (fn [] (when (= ::late (deref outcome 10000 ::late))
                              (println "FAIL: the fiber's read held the main carrier, so the server fiber never answered")
                              (flush) (System/exit 1)))))
    (let [got (jolt.host/main-carrier-run!
                (fn []
                  (let [ss (java.net.ServerSocket. 0 50 (java.net.InetAddress/getLoopbackAddress))
                        fd (net/connect "127.0.0.1" (.getLocalPort ss) 0)
                        s (.accept ss)
                        ;; the server: a fiber on the same carrier, answering once the request came
                        served (binding [a/*go-backend* :fiber]
                                 (a/go (let [in (.getInputStream s) b (byte-array 64)]
                                         (.read in b 0 64)
                                         (.write (.getOutputStream s) (.getBytes "pong" "UTF-8") 0 4)
                                         :served)))]
                    (net/send-bytes fd (.getBytes "ping" "UTF-8"))
                    (let [r (net/recv-bytes fd)]
                      (a/<!! served)
                      (net/close fd) (.close s) (.close ss)
                      (String. ^bytes r "UTF-8")))))]
      (deliver outcome got)
      (if (= "pong" got)
        (do (println "FIBER-READ OK") (flush) (System/exit 0))
        (do (println "FAIL: got" (pr-str got)) (flush) (System/exit 1))))))
