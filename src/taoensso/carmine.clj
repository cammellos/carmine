(ns taoensso.carmine "Clojure Redis client & message queue."
  {:author "Peter Taoussanis"}
  (:refer-clojure :exclude [time get set key keys type sync sort eval])
  (:require [clojure.string       :as str]
            [taoensso.encore      :as encore]
            [taoensso.timbre      :as timbre]
            [taoensso.nippy.tools :as nippy-tools]
            [taoensso.carmine
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)]))

;;;; Encore version check

(let [min-encore-version 1.28] ; For `backport-run!` support
  (if-let [assert! (ns-resolve 'taoensso.encore 'assert-min-encore-version)]
    (assert! min-encore-version)
    (throw
      (ex-info
        (format
          "Insufficient com.taoensso/encore version (< %s). You may have a Leiningen dependency conflict (see http://goo.gl/qBbLvC for solution)."
          min-encore-version)
        {:min-version min-encore-version}))))

;;;; Connections

(encore/defalias with-replies protocol/with-replies)

(defmacro wcar
  "Evaluates body in the context of a fresh thread-bound pooled connection to
  Redis server. Sends Redis commands to server as pipeline and returns the
  server's response. Releases connection back to pool when done.

  `conn-opts` arg is a map with connection pool and spec options:
    {:pool {} :spec {:host \"127.0.0.1\" :port 6379}} ; Default
    {:pool {} :spec {:uri \"redis://redistogo:pass@panga.redistogo.com:9475/\"}}
    {:pool {} :spec {:host \"127.0.0.1\" :port 6379
                     :password \"secret\"
                     :timeout-ms 6000
                     :db 3}}

  A `nil` or `{}` `conn-opts` will use defaults. A `:none` pool can be used
  to skip connection pooling (not recommended).
  For other pool options, Ref. http://goo.gl/e1p1h3,
                               http://goo.gl/Sz4uN1 (defaults).

  Note that because of thread-binding, you'll probably want to avoid lazy Redis
  command calls in `wcar`'s body unless you know what you're doing. Compare:
  `(wcar {} (for   [k [:k1 :k2]] (car/set k :val))` ; Lazy, NO commands run
  `(wcar {} (doseq [k [:k1 :k2]] (car/set k :val))` ; Not lazy, commands run

  See also `with-replies`."
  {:arglists '([conn-opts :as-pipeline & body] [conn-opts & body])}
  [conn-opts & sigs] ; [conn-opts & [s1 & sn :as sigs]]
  `(let [[pool# conn#] (conns/pooled-conn ~conn-opts)

         ;; To support `wcar` nesting with req planning, we mimic
         ;; `with-replies` stashing logic here to simulate immediate writes:
         ?stashed-replies#
         (when protocol/*context*
           (protocol/execute-requests :get-replies :as-pipeline))]

     (try
       (let [response# (protocol/with-context conn#
                         (protocol/with-replies ~@sigs))]
         (conns/release-conn pool# conn#)
         response#)

       (catch Throwable t# ; nb Throwable to catch assertions, etc.
         (conns/release-conn pool# conn# t#) (throw t#))

       ;; Restore any stashed replies to preexisting context:
       (finally
         (when ?stashed-replies#
           (parse nil ; Already parsed on stashing
             (encore/backport-run! return ?stashed-replies#)))))))

;;;; Misc core

(encore/defalias as-bool     encore/as-?bool)
(encore/defalias as-int      encore/as-?int)
(encore/defalias as-float    encore/as-?float)
(encore/defalias as-map      encore/as-map)
(encore/defalias parse       protocol/parse)
(encore/defalias parser-comp protocol/parser-comp)

;;; Note that 'parse' has different meanings in Carmine/Encore context:
(defmacro parse-int     [& body] `(parse as-int    ~@body))
(defmacro parse-float   [& body] `(parse as-float  ~@body))
(defmacro parse-bool    [& body] `(parse as-bool   ~@body))
(defmacro parse-keyword [& body] `(parse keyword   ~@body))

(defmacro parse-suppress "Experimental." [& body]
  `(parse (fn [_#] protocol/suppressed-reply-kw) ~@body))

(comment (wcar {} (parse-suppress (ping)) (ping) (ping)))

(encore/defalias parse-raw   protocol/parse-raw)
(encore/defalias parse-nippy protocol/parse-nippy)

(defmacro parse-map [form & [kf vf]] `(parse #(encore/as-map % ~kf ~vf) ~form))

(defn key
  "Joins parts to form an idiomatic compound Redis key name. Suggested style:
    * \"category:subcategory:id:field\" basic form.
    * Singular category names (\"account\" rather than \"accounts\").
    * Plural _field_ names when appropriate (\"account:friends\").
    * Dashes for long names (\"email-address\" rather than \"emailAddress\", etc.)."
  [& parts] (str/join ":" (mapv #(if (keyword? %) (encore/fq-name %) (str %))
                                parts)))

(comment (key :foo/bar :baz "qux" nil 10))

(encore/defalias raw            protocol/raw)
(encore/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(encore/defalias freeze         nippy-tools/wrap-for-freezing
  "Forces argument of any type (incl. keywords, simple numbers, and binary types)
  to be subject to automatic de/serialization with Nippy.")

(encore/defalias return protocol/return)
(comment (wcar {} (return :foo) (ping) (return :bar))
         (wcar {} (parse name (return :foo)) (ping) (return :bar)))

;;;; Standard commands

(commands/defcommands) ; This kicks ass - big thanks to Andreas Bielk!

(defn redis-call
  "Sends low-level requests to Redis. Useful for DSLs, certain kinds of command
  composition, and for executing commands that haven't yet been added to the
  official `commands.json` spec.

  (redis-call [:set \"foo\" \"bar\"] [:get \"foo\"])"
  [& requests]
  (encore/backport-run!
    (fn [[cmd & args]]
      (let [cmd-parts (-> cmd name str/upper-case (str/split #"-"))
            request   (into (vec cmd-parts) args)]
        (commands/enqueue-request request (count cmd-parts))))
    requests))

(comment (wcar {} (redis-call [:set "foo" "bar"] [:get "foo"]
                              [:config-get "*max-*-entries*"])))

;;;; Misc helpers
;; These are pretty rusty + kept around mostly for back-compatibility


(defn info* "Like `info` but automatically coerces reply into a hash-map."
  [& [clojureize?]]
  (->> (info)
       (parse
        (fn [reply]
          (let [m (->> reply str/split-lines
                       (map #(str/split % #":"))
                       (filter #(= (count %) 2))
                       (into {}))]
            (if-not clojureize?
              m
              (reduce-kv (fn [m k v] (assoc m (keyword (str/replace k "_" "-")) v))
                         {} (or m {}))))))))

(comment (wcar {} (info* :clojurize)))

;; Adapted from redis-clojure
(defn- parse-sort-args [args]
  (loop [out [] remaining-args (seq args)]
    (if-not remaining-args
      out
      (let [[type & args] remaining-args]
        (case type
          :by    (let [[pattern & rest] args]      (recur (conj out "BY" pattern) rest))
          :limit (let [[offset count & rest] args] (recur (conj out "LIMIT" offset count) rest))
          :get   (let [[pattern & rest] args]      (recur (conj out "GET" pattern) rest))
          :mget  (let [[patterns & rest] args]     (recur (into out (interleave (repeat "GET")
                                                                      patterns)) rest))
          :store (let [[dest & rest] args]         (recur (conj out "STORE" dest) rest))
          :alpha (recur (conj out "ALPHA") args)
          :asc   (recur (conj out "ASC")   args)
          :desc  (recur (conj out "DESC")  args)
          (throw (ex-info (str "Unknown sort argument: " type) {:type type})))))))


;;;; Persistent stuff (monitoring, pub/sub, etc.)
;; Once a connection to Redis issues a command like `p/subscribe` or `monitor`
;; it enters an idiosyncratic state:
;;
;;     * It blocks while waiting to receive a stream of special messages issued
;;       to it (connection-local!) by the server.
;;     * It can now only issue a subset of the normal commands like
;;       `p/un/subscribe`, `quit`, etc. These do NOT issue a normal server reply.
;;
;; To facilitate the unusual requirements we define a Listener to be a
;; combination of persistent, NON-pooled connection and threaded message
;; handler.

(declare close-listener)

(defrecord Listener [connection handler state]
  java.io.Closeable
  (close [this] (close-listener this)))

(defmacro with-new-listener
  "Creates a persistent[1] connection to Redis server and a thread to listen for
  server messages on that connection.

  Incoming messages will be dispatched (along with current listener state) to
  (fn handler [reply state-atom]).

  Evaluates body within the context of the connection and returns a
  general-purpose Listener containing:
    1. The underlying persistent connection to facilitate `close-listener` and
       `with-open-listener`.
    2. An atom containing the function given to handle incoming server messages.
    3. An atom containing any other optional listener state.

  Useful for Pub/Sub, monitoring, etc.

  [1] You probably do *NOT* want a :timeout for your `conn-spec` here."
  [conn-spec handler initial-state & body]
  `(let [handler-atom# (atom ~handler)
         state-atom#   (atom ~initial-state)
         {:as conn# in# :in} (conns/make-new-connection
                              (assoc (conns/conn-spec ~conn-spec)
                                :listener? true))]

     (future-call ; Thread to long-poll for messages
      (bound-fn []
        (while true ; Closes when conn closes
          (let [reply# (protocol/get-unparsed-reply in# {})]
            (try
              (@handler-atom# reply# @state-atom#)
              (catch Throwable t#
                (timbre/error t# "Listener handler exception")))))))

     (protocol/with-context conn# ~@body
       (protocol/execute-requests (not :get-replies) nil))
     (Listener. conn# handler-atom# state-atom#)))

(defmacro with-open-listener
  "Evaluates body within the context of given listener's preexisting persistent
  connection."
  [listener & body]
  `(protocol/with-context (:connection ~listener) ~@body
     (protocol/execute-requests (not :get-replies) nil)))

(defn close-listener [listener] (conns/close-conn (:connection listener)))

(defmacro with-new-pubsub-listener
  "A wrapper for `with-new-listener`.

  Creates a persistent[1] connection to Redis server and a thread to
  handle messages published to channels that you subscribe to with
  `subscribe`/`psubscribe` calls in body.

  Handlers will receive messages of form:
    [<msg-type> <channel/pattern> <message-content>].

  (with-new-pubsub-listener
    {} ; Connection spec, as per `wcar` docstring [1]
    {\"channel1\" (fn [[type match content :as msg]] (prn \"Channel match: \" msg))
     \"user*\"    (fn [[type match content :as msg]] (prn \"Pattern match: \" msg))}
    (subscribe \"foobar\") ; Subscribe thread conn to \"foobar\" channel
    (psubscribe \"foo*\")  ; Subscribe thread conn to \"foo*\" channel pattern
   )

  Returns the Listener to allow manual closing and adjustments to
  message-handlers.

  [1] You probably do *NOT* want a :timeout for your `conn-spec` here."
  [conn-spec message-handlers & subscription-commands]
  `(with-new-listener (assoc ~conn-spec :pubsub-listener? true)

     ;; Message handler (fn [message state])
     (fn [[_# source-channel# :as incoming-message#] msg-handlers#]
       (when-let [f# (clojure.core/get msg-handlers# source-channel#)]
         (f# incoming-message#)))

     ~message-handlers ; Initial state
     ~@subscription-commands))



