;;;; cl-actor.asd

(asdf:defsystem #:cl-actor
  :description "Describe cl-actor here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:alexandria
               #:bordeaux-threads)
  :serial t
  :components ((:file "package")
               (:file "cl-actor")))
