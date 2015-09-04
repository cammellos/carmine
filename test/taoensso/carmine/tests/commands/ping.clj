(ns taoensso.carmine.tests.commands.ping
  (:require [clojure.string :as str]
            [clojure.test     :as test :refer :all]
            [taoensso.carmine.tests.helpers :as helpers]
            [taoensso.carmine :as car  :refer (wcar)]))

(deftest ping
  (testing "ping"
    (is (= "PONG"    (helpers/dq1 (car/ping))) "returns PONG")))


