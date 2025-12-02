# MBU Udskrivning 22 år – Workqueue Processing Service

version 2.0.0

---

## 🔍 Overview

This project defines a Python-based **Windows Service** developed for Aarhus Kommune’s MBU automation platform.  
The service runs continuously in the background and automatically fetches and processes **workitems** from the **Automation Server (ATS)** related to the *“Udskrivning 22 år”* workflow.

Its main purpose is to ensure that submitted forms and completed tasks in ATS are retrieved, processed, and passed on to the appropriate downstream systems without manual intervention.

---

## ⚙️ Main Responsibilities

- Run as a background Windows service on a dedicated VM  
- Periodically poll the ATS for new workitems  
- Handle and process data from two main queues:
  - `faglig_vurdering_udfoert`
  - `formular_indsendt`
- Delegate work to the corresponding helper modules in `/helpers`

---
