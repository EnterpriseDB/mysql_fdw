Contributing to `mysql_fdw`
===========================

Following these guidelines helps to facilitate relevant discussion in
pull requests and issues so the developers managing and developing this
open source project can address patches and bugs as efficiently as
possible.


Using Issues
------------

`mysql_fdw`'s maintainers prefer that bug reports, feature requests, and
pull requests are submitted as [GitHub Issues][1].


Bug Reports
-----------

Before opening a bug report:

  1. Search for a duplicate issue using GitHub's issue search
  2. Check whether the bug remains in the latest `master` or `develop`
     commit
  3. Create a reduced test case: remove code and data not relevant to
     the bug

A contributor should be able to begin work on your bug without asking
too many followup questions. If you include the following information,
your bug will be serviced more quickly:

  * Short, descriptive title
  * Your OS
  * Versions of dependencies
  * Any custom modifications

Once the background information is out of the way, you are free to
present the bug itself. You should explain:

  * Steps you took to exercise the bug
  * The expected outcome
  * What actually occurred


Feature Requests
----------------

We are open to adding features but ultimately control the scope and aims
of the project. If a proposed feature is likely to incur high testing,
maintenance, or performance costs it is also unlikely to be accepted.
If a _strong_ case exists for a given feature, we may be persuaded on
merit. Be specific.


Pull Requests
-------------

Well-constructed pull requests are very welcome. By _well-constructed_,
we mean they do not introduce unrelated changes or break backwards
compatibility. Just fork this repo and open a request against `develop`.

Some examples of things likely to increase the likelihood a pull request
is rejected:

  * Large structural changes, including:
    * Re-factoring for its own sake
	* Adding languages to the project
  * Unnecessary whitespace changes
  * Deviation from obvious conventions
  * Introduction of incompatible intellectual property

Please do not change version numbers in your pull request: they will be
updated by the project owners prior to the next release.


License
-------

By submitting a patch, you agree to allow the project owners to license
your work under the terms of the [`LICENSE`][2]. Additionally, you grant
the project owners a license under copyright covering your contribution
to the extent permitted by law. Finally, you confirm that you own said
copyright, have the legal authority to grant said license, and in doing
so are not violating any grant of rights you have made to third parties,
including your employer.

[1]: https://github.com/EnterpriseDB/mysql_fdw/issues
[2]: LICENSE
