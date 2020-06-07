(lang dune 2.5)

(name doughnut)

(generate_opam_files true)

(maintainers "mefyl <mefyl@gruntech.org>")

(authors "mefyl <mefyl@gruntech.org>")

(source
 (github mefyl/doughnut))

(package
 (synopsis "DHT implementations")
 (description "Set of DHT implementations with a common interface.")
 (name doughnut)
 (depends
  (alcotest
   (and
    (>= 1.1.0)
    :with-test))
  (base
   (>= v0.13.2))
  (csexp
   (>= 1.2.0))
  (logs
   (>= 0.7.0))
  (lwt
   (>= 5.3.0))
  (lwt_ppx
   (>= 2.0.1))
  (ocamlformat
   (and
    (= 0.14.2)
    :dev))
  (semver
   (>= 0.1.0))))
