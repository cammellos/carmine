(ns taoensso.carmine.tests.helpers
  (:require [clojure.string :as str]
            [clojure.test     :as test :refer :all]
            [taoensso.carmine.tests.helpers :as helpers]
            [taoensso.carmine :as car  :refer (wcar)]))

(defmacro dq1 [& body] `(car/wcar {:pool {} :spec {:port 7711}} ~@body))

(defn- flush-db! []
  (dq1 (car/debug-flushall)))

(defn setup-teardown-db! [f]
  (flush-db!)
  (f)
  (flush-db!))
