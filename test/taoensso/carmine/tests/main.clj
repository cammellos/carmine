(ns taoensso.carmine.tests.main
  (:require [clojure.string :as str]
            [clojure.test     :as test :refer :all]
            [taoensso.encore  :as encore]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.commands   :as commands]
            [taoensso.carmine.protocol   :as protocol]
            [taoensso.carmine.benchmarks :as benchmarks]))

(comment (test/run-tests '[taoensso.carmine.tests.main]))

(defmacro dq1 [& body] `(car/wcar {:pool {} :spec {:port 7711}} ~@body))

(defn- flush-db! []
  (dq1 (car/debug-flushall)))

(defn- setup-teardown-db! [f]
  (flush-db!)
  (f)
  (flush-db!))


(use-fixtures :each setup-teardown-db!)

(deftest ping
  (testing "ping"
    (is (= "PONG"    (dq1 (car/ping))) "returns PONG")))

(deftest show
  (testing "show"
    (let [id (dq1 (car/addjob "testq" "job1" 5000))
          job-info (dq1 (car/show id))] 
      (is (vector? job-info) "returns a vector")
      (is (= id (get (apply hash-map job-info) "id")) "returns the id of the job")
      (is (= "testq" (get (apply hash-map job-info) "queue")) "returns the queue of the job")
      (is (= "queued" (get (apply hash-map job-info) "state")) "returns the state of the job"))))

(deftest addjob-replicate
  (testing "addjob"
    (testing "accepts a REPLICATE option"
      (let [
            id (dq1 (car/addjob "testq" "job1" 5000 "REPLICATE" 1))
            job-info (apply hash-map (dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 1 (count (get job-info "nodes-delivered"))) "replicates to a single node")
        (is (= "queued" (get job-info "state")) "queues the job")))))

(deftest addjob-delay
  (testing "addjob"
    (testing "accepts a DELAY option"
      (let [
            id (dq1 (car/addjob "testq" "job1" 5000 "DELAY" 20000))
            job-info (apply hash-map (dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 3 (count (get job-info "nodes-delivered"))) "replicates to multiple nodes")
        (is (= "active" (get job-info "state")) "does not queue the job")))))

(deftest addjob-delay
  (testing "addjob"
    (testing "accepts a TTL option"
      (let [
            id (dq1 (car/addjob "testq" "job1" 5000 "TTL" 10))
            job-info (apply hash-map (dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 3 (count (get job-info "nodes-delivered"))) "replicates to multiple nodes")
        (is (= "queued" (get job-info "state")) "queues the job")))))

(deftest addjob-async
  (testing "addjob"
    (testing "accepts an ASYNC option"
      (let [
            id (dq1 (car/addjob "testq" "job1" 5000 "ASYNC"))
            job-info (apply hash-map (dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 3 (count (get job-info "nodes-delivered"))) "replicates to multiple nodes")
        (is (= "queued" (get job-info "state")) "queues the job")))))

(deftest addjob-async
  (testing "addjob"
    (testing "accepts an MAXLEN option"
      (let [
            id (dq1 (car/addjob "testq" "job1" 5000 "MAXLEN" 1))
            id2 (dq1 (car/addjob "testq" "job1" 5000 "MAXLEN" 1)) ; Currently there's a bug on disque
            job-info (apply hash-map (dq1 (car/show id)))] 
        (is (string? id) "returns an id string")
        (is (not-empty id) "returns an non-empty id")
        (is (= 3 (count (get job-info "nodes-delivered"))) "replicates to multiple nodes")
        (is (= "queued" (get job-info "state")) "queues the job")
        (is (thrown? clojure.lang.ExceptionInfo (dq1 (car/addjob "testq" "job1" 5000 "MAXLEN" 1))))))))
