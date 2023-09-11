---
name: "New Product Build"
about: Notification For New Attempted Build of Product
title: "New Product Build - {{ env.PRODUCT }}:{{ env.PRODUCT_VERSION }}: {{ env.SUMMARY_NOTE }}"
labels: ["data update"]
assignees: []

---

# New Product Build - {{ env.PRODUCT }}:{{ env.PRODUCT_VERSION }}: {{ env.SUMMARY_NOTE }}

Action Required: {{ env.REQUIRED_ACTION }}

Link to build is [here]({{ env.BUILD_URL }})
