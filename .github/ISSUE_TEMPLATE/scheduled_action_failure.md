---
name: "Scheduled Action Failure"
about: Notification For failed attempt of scheduled github action
title: "Scheduled Action Failure - {{ env.ACTION }}"
labels: ["bug"]
assignees: []

---
[Failed action]({{ env.BUILD_URL }})
