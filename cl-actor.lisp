;;;; cl-actor.lisp

(in-package #:cl-actor)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (a-cl-logger:define-logger alog ())
  (use-package :bordeaux-threads))

;;; "cl-actor" goes here. Hacks and glory await!

;;; Exceptions
(define-condition path-already-exists (error)
  ((text :initarg :text :reader text)))

(define-condition actor-doesnt-exists (error)
  ((text :reader text :initform "actor doesn't exists")))
;;;

(defclass actor-system ()
  ((actors :accessor get-actors
           :initform (make-hash-table :test 'equal))
   (lock :accessor lock-of
         :initform (make-recursive-lock "system-lock")))
  (:documentation
     "Holds the actors instances, creates actor refs
     and takes care of delivering the messages to the right actor"))

(defun make-system ()
  "Creates the actor system"
  (make-instance 'actor-system))

(defclass actor ()
  ((queue :accessor queue-of
          :initform '())
   (lock :initform (make-recursive-lock "queue-lock")
         :accessor lock-of)
   (scheduler :reader scheduler-of
              :initform nil
              :initarg :scheduler)
   (state :accessor state-of
          :initform (list 'default))
   (ref :accessor ref-of)))

(defun make-actor (scheduler)
  (make-instance 'actor :scheduler scheduler))

(defclass actor-ref ()
  ((path :reader get-path
         :initarg :path))
  (:documentation
   "Reference to the real actor through path"))

(defun make-ref (path)
  "Create the actor ref with the given path"
  (make-instance 'actor-ref :path path))

(defclass scheduler () ())
(defclass pool-scheduler (scheduler)
  ((active :reader get-active
           :initform (make-hash-table :test 'equal))
   (pool-size :reader get-pool-size
              :initarg :pool-size)
   (pool :reader get-pool
         :initform nil)
   (lock :reader lock-of
         :initform (make-recursive-lock "pool-scheduler-lock"))))

(defun make-pool-scheduler (pool-size)
  (make-instance 'pool-scheduler :pool-size pool-size))

;; (defmethod initialize-instance :around ((s pool-scheduler) &key)
;;   )

(defgeneric add-actor (system path actor))
(defgeneric get-actor (system ref))
(defgeneric actor-of (system actor &key name))
(defgeneric send (system ref message sender))
(defgeneric schedule (system scheduler path actor))
(defgeneric run (actor))

(defgeneric become (actor state))
(defgeneric unbecome (actor))
(defgeneric receive (actor message sender state))

(defmethod add-actor ((system actor-system) path (actor actor))
  "Add a ref into the system throwing if it already exists"
  (with-lock-held ((lock-of system))
    (let* ((actors (get-actors system))
           (key (gethash path actors)))
      (if key
          (error 'path-already-exists :text (format nil "path ~A already esists" path))
          (setf (gethash path actors) actor)))))

(defmethod get-actor ((system actor-system) (ref actor-ref))
  "Returns the actor associated with the ref"
  (with-recursive-lock-held ((lock-of system))
    (let* ((actors (get-actors system))
           (actor (gethash (get-path ref) actors)))
      (unless actor
        (error 'actor-doesnt-exists))
      actor)))

(defun random-string (length)
  "Generates a random string of length"
  (let ((chars "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"))
    (coerce (loop repeat length collect (aref chars (random (length chars))))
            'string)))

(defun make-path (base name)
  "Create a path from a base path and a name"
  (unless name
    (setf name (random-string 10)))
  (if base
      (format nil "~A/~A" base name)
      (format nil "/~A" name)))

(defmethod run ((actor actor))
  "Run the actor, extracting a message from its own queue and processing it"
  (labels ((pop-queue ()
             (with-lock-held ((lock-of actor))
               (let ((queue (queue-of actor)))
                 (when queue
                   (let ((head (car queue)))
                     (setf (queue-of actor) (cdr queue))
                     head)))))
           (queue-has-elem? ()
             (with-lock-held ((lock-of actor))
               (if (queue-of actor) t nil))))
    (let ((message (pop-queue)))
      (when message
        (receive actor (car message) (car (state-of actor))  (cadr message))
        (when (queue-has-elem?)
          (run actor))))))

(defmethod receive ((actor actor) message state sender)
  (let ((*standard-output* *stdout*))
    (format t "~A didn't receive message \"~A\" from ~A" actor message sender)))

(defmethod become ((actor actor) (state symbol))
  "Push a new state on the stack"
  (let ((states (state-of actor)))
    (setf (state-of actor) (cons state states))))

(defmethod unbecome ((actor actor))
  "Pops the state of the actor if the state size is > 1"
  (let ((states (state-of actor)))
    (when (> (length states) 1)
      (setf (state-of actor) (cdr states)))))

(defmethod schedule ((system actor-system) (scheduler pool-scheduler) path (actor actor))
  "Schedule the run of the actor on the scheduler"
  (with-lock-held ((lock-of scheduler))
    (let* ((active (get-active scheduler))
           (is-active (gethash path active)))
      (unless is-active
        (setf (gethash path active) t)
        (make-thread
         (lambda ()
           (run actor)
           (with-lock-held ((lock-of scheduler))
             (setf (gethash path active) nil))))))))

(defmethod send ((system actor-system) (ref actor-ref) message (sender actor-ref))
  "Send the message to the actor referred by the ref"
  (let ((actor (get-actor system ref)))
    (with-lock-held ((lock-of actor))
      (let* ((queue (queue-of actor))
             (path (get-path ref)))
        (setf (queue-of actor) (append queue (list (list message sender))))
        (schedule system (scheduler-of actor) path actor)))))

(defmethod actor-of ((system actor-system) (actor actor) &key name)
  "Create an actor ref for the actor passed and inserts it into the system"
  (alog.debug "creating actor ~a" name)
  (let* ((path (make-path nil name))
         (ref (make-ref path)))
    (with-lock-held ((lock-of actor))
      (setf (ref-of actor) ref))
    (add-actor system path actor)
    ref))

(defparameter *stdout* *standard-output*)
(defparameter *system* (make-system))
(defparameter *pool* (make-pool-scheduler 10))

(defmacro defactor (name &body body)
  "Defines a new actor, providing a better primitives for defining
receive and state changing"
  (let ((this (gensym))
        (st (gensym))
        (sd (gensym)))
    `(macrolet ((receive (message state &body body)
                  `(defmethod receive ((,',this ,',name)
                                       ,message
                                       (,',st (eql ,state))
                                       (,',sd actor-ref))
                     (flet ((become (state)
                              (become ,',this state))
                            (unbecome ()
                              (unbecome ,',this))
                            (get-self ()
                              (ref-of ,',this))
                            (get-sender ()
                              ,',sd))
                       ,@body))))
       (progn
         (defclass ,name (actor) ())
         ,@body))))

(defactor dead-letter
  (receive message 'default
           (let ((*standard-output* *stdout*))
             (format t "message: \"~A\" from ~A deadletter" message (get-sender)))))

(defparameter *dead-letter* (actor-of *system* (make-instance 'dead-letter :scheduler *pool*)))

(defactor actor-1
  (receive (message string) 'default
           (let ((*standard-output* *stdout*))
             (format t "Actor1: ~A~%" message))
           (send *system* (get-sender) 1 (get-self))
           (become 'state1))

  (receive (message number) 'state1
           (let ((*standard-output* *stdout*))
             (format t "Actor1: state1 ~A~%" message)
             (unbecome))))

(defparameter *ref1* (actor-of *system* (make-instance 'actor-1 :scheduler *pool*)))
(send *system* *ref1* "prova" *dead-letter*)
(send *system* *ref1* 1 *dead-letter*)
