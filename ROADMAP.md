# Chronological Implementation Roadmap (API → Bronze → Silver)

Day numbers are **relative working days**. No deadline appears in our files, so stretch or squeeze the plan to fit your real submission date. Infrastructure (M1), API investigation (M2), schema design (M3) and rule design (M4) all **start on Day 1 in parallel**.

M1 = Azure/Repo · M2 = API/Bronze · M3 = Silver · M4 = DQ/Orchestration

---

## Phase 0 – Kick-off and decisions (Day 1)
| Item | Detail |
|---|---|
| **To do** | Assign Member 1–4 names. Read README Sections 3, 4, 12. Send the instructor the Silver-only / PostgreSQL question. M2 replaces the exposed GFW token. M1 checks that Databricks can be created on your student subscription |
| **Who** | Whole team (M1 and M2 lead their parts) |
| **Resources** | README, Azure Portal, GFW API portal, email to instructor |
| **Depends on** | Nothing |
| **Output** | Names in README, question sent, new token ready, Databricks feasibility known |
| **Verify** | `docs/instructor_decision.md` exists (even if "waiting"); old token no longer used; M1 reports "Databricks OK" or agrees on the fallback |

## Phase 1A – Azure and repository setup (Day 1–3) · *parallel with 1B, 1C, 1D*
| Item | Detail |
|---|---|
| **To do** | Resource group, storage (HNS on), 3 containers, Key Vault, service principal, roles, Databricks, secret scope, cluster, budget alerts. GitHub repo, protections, skeleton, `.gitignore`, templates, CI, `common/` and config templates |
| **Who** | M1 |
| **Resources** | Azure Portal, GitHub, Databricks (README Section 7 and 8) |
| **Depends on** | Phase 0 |
| **Output** | Working environment; repo skeleton merged to `main`; `00_setup_check.py` |
| **Verify** | All four members run `00_setup_check` and see read/write on `bronze`, `silver`, `audit`; budget alert test email arrives; direct push to `main` is blocked |

## Phase 1B – API investigation (Day 1–3) · *parallel*
| Item | Detail |
|---|---|
| **To do** | Call each endpoint on a tiny window; save samples; answer every "verify" item (README 9.4, 22); time one port-month; write `docs/api_notes.md`; put new token in Key Vault when M1 is ready |
| **Who** | M2 |
| **Resources** | GFW token, local Python, `marineapi.py` as reference (**not committed**) |
| **Depends on** | New token only. Key Vault storage waits for M1 |
| **Output** | `docs/api_notes.md`, 1–2 small `sample_data/` JSON files, load-time estimate |
| **Verify** | Each "verify" question is answered or marked "unknown" with a reason; samples contain no token |

## Phase 1C – Schema and rule design (Day 1–5) · *parallel*
| Item | Detail |
|---|---|
| **To do** | M3: draft Bronze read schemas and Silver schemas from README 9.3 and 11.2. M4: draft `rules.yaml`, audit table design and logger interface |
| **Who** | M3, M4 |
| **Resources** | README Sections 9, 11, 16; M2's samples when ready |
| **Depends on** | Light: samples from M2 (Day 3) improve accuracy |
| **Output** | Draft `schemas/silver_schemas.py`, draft `docs/silver_data_dictionary.md`, draft `data_quality/rules.yaml`, audit interface |
| **Verify** | M3 and M4 review each other's drafts; every column in the data dictionary has a type, nullability and rule |

## Phase 2 – Contracts frozen (Day 3–6)
| Item | Detail |
|---|---|
| **To do** | Freeze contracts C1, C2, C6 (README 20.1) and review C3. Merge first PRs: skeleton (M1), Bronze layout doc and samples (M2) |
| **Who** | M1, M2, M3, M4 |
| **Resources** | GitHub PRs |
| **Depends on** | Phases 1A–1C |
| **Output** | Merged `common/`, config template, Bronze layout, notebook parameter list |
| **Verify** | Everyone's branch has merged `main`; no one uses a private copy of a contract |

## Phase 3 – Bronze implementation (Day 4–10)
| Item | Detail |
|---|---|
| **To do** | Refactor script into `ingestion/` modules; build Bronze notebooks; retries, `.part` rename, manifest, watermark; run a small sample load, then the **initial load**; then an **incremental** run |
| **Who** | M2 (M1 helps with Azure access issues; M4 reviews error handling) |
| **Resources** | Databricks cluster or laptop (see README 19), ADLS `bronze`, Key Vault token |
| **Depends on** | Storage + secret scope (M1, Phase 1A) |
| **Output** | Raw JSONL for 5 event types + vessels in `bronze`; manifest; watermark; `docs/bronze_load_report.md` |
| **Verify** | Manifest shows all chunks `SUCCESS`; re-running a finished window creates no new files; incremental run requests only the new window; unit tests (mocked) pass |

## Phase 4 – Silver core (Day 6–14) · *starts on samples, switches to real Bronze*
| Item | Detail |
|---|---|
| **To do** | M3: helpers (**publish by Day 8**), `stg_port_visits`, `stg_vessel_events`, `sql/10_stg_tables.sql`, MERGE writes, rejects. M4: DQ engine, schema validation, audit logger (final by Day 10) |
| **Who** | M3, M4 |
| **Resources** | Bronze samples then real Bronze; Delta tables in `silver` |
| **Depends on** | C2 (Bronze layout), C5 (audit interface), M1's `00_create_schemas.sql` |
| **Output** | Two Silver tables populated; helper library; audit tables; DQ engine |
| **Verify** | Bronze unique ids = Silver rows + rejects; running twice gives the same row counts; `transform_utils` tests pass; no `inferSchema` in the repo |

## Phase 5 – Remaining Silver tables (Day 10–16)
| Item | Detail |
|---|---|
| **To do** | M1: `stg_ais_gaps`. M2: `stg_vessels` (needs Bronze vessels). Both reuse M3's helpers and audit logger. M4 adds their rules |
| **Who** | M1, M2 (reviewed by M3), M4 |
| **Resources** | `silver/transform_utils.py`, `common/audit.py` |
| **Depends on** | Phase 4 helpers; Phase 3 Bronze vessels |
| **Output** | `stg_ais_gaps`, `stg_vessels` populated |
| **Verify** | Same checks as Phase 4 for each table; M3 approves both PRs |

## Phase 6 – Orchestration and integration testing (Day 14–20)
| Item | Detail |
|---|---|
| **To do** | M4 builds the Workflow job (`bronze_events → bronze_vessels → silver tasks → dq_checks`), failure emails, retries; runs the 7 integration scenarios; logs bugs as GitHub Issues; owners fix them |
| **Who** | M4 leads; M1, M2, M3 fix their bugs |
| **Resources** | Databricks Workflows (job cluster), all notebooks |
| **Depends on** | All Bronze and Silver notebooks merged |
| **Output** | Green end-to-end run; `orchestration/workflow_job.json`; `tests/integration_test_plan.md` with results; `docs/runbook.md` |
| **Verify** | Job succeeds start to finish from the Git `main` branch; scenarios 1–7 recorded with pass/fail; no critical DQ rule fails |

## Phase 7 – Reports and documentation (Day 18–23)
| Item | Detail |
|---|---|
| **To do** | M1: Data Cleaning Report PDF. M4: Data Quality Report PDF. M3: finalize Silver data dictionary. M2: Bronze load report. M1: final requirement-mapping doc and cost table |
| **Who** | All (M1 and M4 lead the PDFs) |
| **Resources** | Audit tables, DQ results, screenshots |
| **Depends on** | Phase 6 final run |
| **Output** | `reports/data_cleaning_report.pdf`, `reports/data_quality_report.pdf`, final docs |
| **Verify** | Report numbers match the audit tables exactly; the Gold-dependent items are listed as "not implemented – scope decision" |

## Phase 8 – Final check and submission (Day 23–25)
| Item | Detail |
|---|---|
| **To do** | Secret scan of files and history; README final pass; every checklist in README Section 13 and 21 ticked; collect screenshots; submit the GitHub link and PDFs. Delete Azure resources **only after** the submission is accepted |
| **Who** | Whole team (M1 coordinates) |
| **Resources** | GitHub, gitleaks/secret scanning, Azure Portal |
| **Depends on** | Phase 7 |
| **Output** | Final `main` branch, submission package |
| **Verify** | A fresh clone contains no secrets; a teammate who did not write the code can follow the README and run the job; each member has merged PRs |

---

## Parallel view

| Days | M1 | M2 | M3 | M4 |
|---|---|---|---|---|
| 1–3 | Azure + repo | API investigation, new token | Schema drafts | Rule + audit design |
| 3–6 | `common/`, contracts | Bronze layout, samples | Schemas final (C3) | Audit stub (C5), parameters (C6) |
| 4–10 | Support, docs, CI | **Bronze build + loads** | Helpers, port visits, events | DQ engine, audit |
| 10–16 | `stg_ais_gaps` | `stg_vessels` | Reviews, fixes, rejects | Rules for all tables |
| 14–20 | Fix bugs | Fix bugs | Fix bugs | **Job + integration tests** |
| 18–25 | Cleaning report, final docs | Bronze report | Data dictionary | DQ report |
