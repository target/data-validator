---
name: New check
about: Submit a new check
title: 'New check:'
labels: 'enhancement'
assignees: ''

---

# What issue is this PR solving?

<!-- Provide the link. -->

# What does the configuration look like?

<!-- 
#### validatorName

Provide a brief description of check and the condition that will result in a failure.  Document any arguments in the table below.
 
| Arg | Type | Description |
|-----|------|-------------| 
-->

# Have you completed all of these?

- [ ] Add configuration documentation to the Validators section of the README.  You should be able to copy this from the previous section.
- [ ] Pass the style checker requirements without warnings or errors (`sbt test` will not work without compliance!)
- [ ] Does not modify any of the other validators. Please review the section Refactoring in the CONTRIBUTING.md.
- [ ] Include tests. Submissions without tests will not be considered. Test the following things:
     - [ ] Configuration parsing
     - [ ] Configuration sanity checking
     - [ ] Variable substitution
     - [ ] Actual check functionality
