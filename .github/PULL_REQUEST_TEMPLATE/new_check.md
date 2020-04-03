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
Provide an example of how you would like the configuration for the check to look. 
Most of our rework requests are the result of an unclear vision for the interface to the check!
-->

# Have you completed all of these?

- [ ] Pass the style checker requirements without warnings or errors (`sbt test` will not work without compliance!)
- [ ] Include tests. Submissions without tests will not be considered. Test the following things:
     - [ ] Configuration parsing
     - [ ] Configuration sanity checking
     - [ ] Variable substitution
     - [ ] Actual check functionality
