(ns taoensso.carmine.tests.commands.addjob
  (:require [clojure.string :as str]
            [clojure.test     :as test :refer :all]
            [taoensso.carmine.tests.helpers :as helpers]
            [taoensso.carmine :as car  :refer (wcar)]))

(use-fixtures :each helpers/setup-teardown-db!)

(deftest addjob-replicate
  (testing "addjob"
    (testing "accepts a REPLICATE option"
      (let [
            id (helpers/dq1 (car/addjob "testq" "addjob-replicate" 5000 "REPLICATE" 1))
            job-info (apply hash-map (helpers/dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 1 (count (get job-info "nodes-delivered"))) "replicates to a single node")
        (is (= "queued" (get job-info "state")) "queues the job")))))

(deftest addjob-delay
  (testing "addjob"
    (testing "accepts a DELAY option"
      (let [
            id (helpers/dq1 (car/addjob "testq" "addjob-delay" 5000 "DELAY" 1))
            job-info (apply hash-map (helpers/dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 3 (count (get job-info "nodes-delivered"))) "replicates to multiple nodes")
        (is (= "active" (get job-info "state")) "does not queue the job")))))

(deftest addjob-ttl
  (testing "addjob"
    (testing "accepts a TTL option"
      (let [
            id (helpers/dq1 (car/addjob "testq" "addjob-ttl" 5000 "TTL" 1))
            job-info (apply hash-map (helpers/dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 3 (count (get job-info "nodes-delivered"))) "replicates to multiple nodes")
        (is (= "queued" (get job-info "state")) "queues the job")))))

(deftest addjob-async
  (testing "addjob"
    (testing "accepts an ASYNC option"
      (let [
            id (helpers/dq1 (car/addjob "testq" "addjob-async" 5000 "ASYNC"))
            job-info (apply hash-map (helpers/dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 3 (count (get job-info "nodes-delivered"))) "replicates to multiple nodes")
        (is (= "queued" (get job-info "state")) "queues the job")))))

(deftest addjob-maxlen
  (testing "addjob"
    (testing "accepts an MAXLEN option"
      (let [
            id (helpers/dq1 (car/addjob "testq" "addjob-maxlen1" 5000 "MAXLEN" 1))
            id2 (helpers/dq1 (car/addjob "testq" "addjob-maxlen2" 5000 "MAXLEN" 1)) ; Currently there's a bug on disque
            job-info (apply hash-map (helpers/dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 3 (count (get job-info "nodes-delivered"))) "replicates to multiple nodes")
        (is (= "queued" (get job-info "state")) "queues the job")
        (is (thrown? clojure.lang.ExceptionInfo (helpers/dq1 (car/addjob "testq" "addjob-maxlen3" 5000 "MAXLEN" 1))))))))
