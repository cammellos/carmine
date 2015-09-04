(ns taoensso.carmine.tests.commands.ackjob
  (:require [clojure.string :as str]
            [clojure.test     :as test :refer :all]
            [taoensso.carmine.tests.helpers :as helpers]
            [taoensso.carmine :as car  :refer (wcar)]))

(use-fixtures :each helpers/setup-teardown-db!)

(deftest ackjob
  (testing "ackjob"
    (testing "takes and id as an argument"
      (let [
            id (helpers/dq1 (car/addjob "testq" "addjob-replicate" 5000 "REPLICATE" 1))
            result (helpers/dq1 (car/ackjob id))]
        (is (= 1 result) "returns the numbers of job acknowledged")))))

(deftest ackjob-multiple-ids
  (testing "ackjob"
    (testing "takes multiple ids as argument"
      (let [
            id1 (helpers/dq1 (car/addjob "testq" "addjob-replicate" 5000 "REPLICATE" 1))
            id2 (helpers/dq1 (car/addjob "testq" "addjob-replicate" 5000 "REPLICATE" 1))
            result (helpers/dq1 (car/ackjob id1 id2))]
        (is (= 2 result) "returns the numbers of job acknowledge")))))
