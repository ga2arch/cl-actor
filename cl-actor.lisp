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
   (ref :accessor ref-of)
   (system :accessor system-of)))

(defun make-actor (scheduler)
  (make-instance 'actor :scheduler scheduler))

(defclass actor-ref ()
  ((path :reader get-path
         :initarg :path)
   (system :accessor system-of
           :initarg :system))
  (:DOCUMENTATION
   "Reference to the real actor through path"))

(defun make-ref (system path)
  "Create the actor ref with the given path"
  (make-instance 'actor-ref :path path :system system))

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
(defgeneric remove-actor (system actor))
(defgeneric get-actor (system ref))
(defgeneric actor-of (system actor &rest args))
(defgeneric send (system ref message sender))
(defgeneric schedule (system scheduler path actor))
(defgeneric run (actor))
(defgeneric stop (actor))

(defgeneric become (actor state))
(defgeneric unbecome (actor))
(defgeneric receive (actor message sender state))

(defparameter *dead-letter* nil)

(defmethod add-actor ((system actor-system) path (actor actor))
  "Add a ref into the system throwing if it already exists"
  (with-lock-held ((lock-of system))
    (let* ((actors (get-actors system))
           (key (gethash path actors)))
      (if key
          (error 'path-already-exists :text (format nil "path ~A already esists" path))
          (setf (gethash path actors) actor)))))

(defmethod remove-actor ((system actor-system) (actor actor))
    (with-lock-held ((lock-of system))
      (setf (gethash (get-path (ref-of actor))
                     (get-actors system))
            (get-actor system *dead-letter*))))

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
        (receive actor (car message) (car (state-of actor)) (cadr message))
        (if (eql 'dead (car (state-of actor)))
            (remove-actor (system-of actor) actor)
            (when (queue-has-elem?)
              (run actor)))))))

(defmethod stop ((actor actor))
  (with-lock-held ((lock-of actor))
    (setf (state-of actor) (list 'dead))))

(defmethod receive ((actor actor) message state sender)
  (let ((*standard-output* *stdout*))
    (format t "~A didn't receive message \"~A\" from ~A" actor message sender)))

(defmethod receive ((actor actor) (message (eql 'poison-pill)) state sender)
  (stop actor))

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

(defmethod send ((system actor-system) (ref actor-ref) message (sender (eql nil)))
  (send system ref message *dead-letter*))

(defmethod send ((system actor-system) (ref actor-ref) message (sender actor-ref))
  "Send the message to the actor referred by the ref"
  (let ((actor (get-actor system ref)))
    (with-lock-held ((lock-of actor))
      (let* ((queue (queue-of actor))
             (path (get-path ref)))
        (setf (queue-of actor) (append queue (list (list message sender))))
        (schedule system (scheduler-of actor) path actor)))))

(defmethod actor-of ((system actor-system) (actor-name symbol) &rest args)
  "Create an actor ref for the actor passed and inserts it into the system"
  (let* ((actor (apply #'make-instance (cons actor-name args)))
         (path (make-path nil nil))
         (ref (make-ref system path)))
    (with-lock-held ((lock-of actor))
      (setf (ref-of actor) ref)
      (setf (system-of actor) system))
    (add-actor system path actor)
    ref))

(defmethod actor-of ((parent actor-ref) (actor-name symbol) &rest args)
  "Create an actor ref for the actor passed and inserts it into the system"
  (let* ((actor (apply #'make-instance (cons actor-name args)))
         (system (system-of parent))
         (path (make-path (get-path parent) nil))
         (ref (make-ref system path)))
    (with-lock-held ((lock-of actor))
      (setf (ref-of actor) ref)
      (setf (system-of actor) system))
    (add-actor system path actor)
    ref))

(defun extract-keys (args)
  (let ((body args)
        (state)
        (scheduler)
        (supervisor)
        (on-start)
        (on-stop))
    (loop
       (unless (and body (cdr body)) (return))
       (case (car body)
         ((:state)
          (setf state (cadr body)))
         ((:supervisor)
          (setf supervisor (cadr body)))
         ((:scheduler)
          (setf scheduler (cadr body)))
         ((:on-start)
          (setf on-start (cadr body)))
         ((:on-stop)
          (setf on-stop (cadr body)))
         (otherwise (return)))
       (setf body (cddr body)))
    (list state scheduler on-start on-stop body)))

(defparameter *stdout* *standard-output*)
(defparameter *system* (make-system))
(defparameter *pool* (make-pool-scheduler 10))

(defmacro defactor (name args &body keys-body)
  "Defines a new actor, providing a better primitives for defining
receive and state changing"
  (destructuring-bind (state scheduler on-start on-stop body) (extract-keys keys-body)
    (let ((this (gensym))
          (st (gensym))
          (sd (gensym)))
      `(let (,@(mapcar #'list state))
         (macrolet ((receive (message state &body body)
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
                                  ,',sd)
                                (get-system ()
                                  (system-of ,',this)))
                           (flet ((send (to message from)
                                    (send (get-system) to message from))
                                  (stop-self ()
                                    (send (get-system) (get-self) 'poison-pill (get-self))))
                            ,@body)))))
           (progn
             (defclass ,name (actor) ())
             (defmethod run ((actor ,name))
               (flet ((actor-of (act &rest args)
                        (apply #'actor-of (append (list (system-of actor) act) args))))
                 ,on-start
                 (call-next-method actor)))
             (defmethod stop ((actor ,name))
               ,on-stop
               (call-next-method actor))
             ,@body))))))

(defactor dead-letter ()
  (receive message 'default
           (let ((*standard-output* *stdout*))
             (format t "message: \"~A\" from ~A deadletter" message (get-sender)))))

(setf *dead-letter* (actor-of *system* 'dead-letter :scheduler *pool*))

(defactor worker ()
  (receive (message string) 'default
           (let ((*standard-output* *stdout*))
             (format t "Worker: ~A~%" message))))

(defactor actor-1 ()
  :state (worker)

  :on-start
  (setf worker (actor-of 'worker :scheduler *pool*))

  :on-stop
  (let ((*standard-output* *stdout*))
    (format t "Actor1: I'm dead~%"))

  (receive (message string) 'default
           (let ((*standard-output* *stdout*))
             (format t "Actor1: ~A~%" message))
           (send (get-self) 1 (get-self))
           (send worker "lol" (get-self))
           (become 'state1))

  (receive (message number) 'state1
           (let ((*standard-output* *stdout*))
             (format t "Actor1: state1 ~A~%" message)
             (stop-self)
             (unbecome))))

(defparameter *ref1* (actor-of *system* 'actor-1 :scheduler *pool*))
(send *system* *ref1* "prova" nil)
