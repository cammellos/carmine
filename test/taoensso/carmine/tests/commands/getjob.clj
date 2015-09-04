(ns taoensso.carmine.tests.commands.getjob
  (:require [clojure.string :as str]
            [clojure.test     :as test :refer :all]
            [taoensso.carmine.tests.helpers :as helpers]
            [taoensso.carmine :as car  :refer (wcar)]))

(use-fixtures :each helpers/setup-teardown-db!)

(deftest getjob
  (testing "getjob"
    (let [
          id (helpers/dq1 (car/addjob "testq" "getjob" 5000 "REPLICATE" 1))
          job-info (helpers/dq1 (car/getjob "FROM" "testq"))] 
      (is (= [["testq" id "getjob"]] job-info) "returns a vector of job definitions"))))

(deftest getjob-multiple-queues
  (testing "getjob"
    (testing "accepts multiple queues"
      (let [
            id (helpers/dq1 (car/addjob "testq2" "getjob-mqueue" 5000 "REPLICATE" 1))
            job-info (helpers/dq1 (car/getjob "FROM" "testq1" "testq2"))] 
        (is (= [["testq2" id "getjob-mqueue"]] job-info) "returns a vector of job definitions")))))

(deftest getjob-count
  (testing "getjob"
    (testing "accepts a COUNT option"
      (let [
            id1 (helpers/dq1 (car/addjob "testq1" "getjob-mqueue1" 5000 "REPLICATE" 1))
            id2 (helpers/dq1 (car/addjob "testq2" "getjob-mqueue2" 5000 "REPLICATE" 1))
            job-info (helpers/dq1 (car/getjob "COUNT" 2 "FROM" "testq1" "testq2"))] 
        (is (= [["testq1" id1 "getjob-mqueue1"] ["testq2" id2 "getjob-mqueue2"]] job-info) "returns COUNT jobs")))))

(deftest getjob-nohang
  (testing "getjob"
    (testing "accepts a NOHANG option"
      (let [job-info (helpers/dq1 (car/getjob "NOHANG" "FROM" "testq"))] 
        (is (nil? job-info) "returns immediately if no jobs are in the queue")))))

(deftest getjob-timeout
  (testing "getjob"
    (testing "accepts a TIMEOUT option"
      (let [job-info (helpers/dq1 (car/getjob "TIMEOUT" 1 "FROM" "testq"))] 
        (is (nil? job-info) "returns after the TIMEOUT if no jobs are in the queue")))))

(deftest getjob-with-counters
  (testing "getjob"
    (testing "accepts a WITHCOUNTERS option"
      (let [
            id (helpers/dq1 (car/addjob "testq" "getjob-mqueue" 5000 "REPLICATE" 1))
            job-info (helpers/dq1 (car/getjob "WITHCOUNTERS" "FROM" "testq"))] 
        (is (some #{"nacks"} (get job-info 0)) "returns the number of acks")
        (is (some #{"additional-deliveries"} (get job-info 0)) "returns the number of additional deliveries")))))
