.. _comparison-dagster-label:

Comparison: ``b2luigi`` vs Dagster
===================================

This page provides a comprehensive comparison between ``b2luigi`` and
`Dagster <https://dagster.io>`_, two workflow-orchestration frameworks written in Python.
The goal is to help you choose the right tool for your project and to understand which
features each framework offers.

Overview
--------

.. list-table::
   :widths: 20 40 40
   :header-rows: 1

   * - Aspect
     - ``b2luigi``
     - Dagster
   * - Purpose
     - Thin extension of ``luigi`` for batch-system submission and HPC workflows
     - General-purpose data-orchestration platform with first-class observability
   * - Core abstraction
     - **Task** (Python class with ``run`` / ``output`` methods)
     - **Asset** (data artefact) or **Op** (unit of compute)
   * - Target audience
     - Scientific-computing users (HEP, physics, large-scale parameter sweeps)
     - Data engineers, ML engineers, analytics teams
   * - Complexity
     - Lightweight, minimal learning curve on top of ``luigi``
     - Feature-rich; steeper learning curve
   * - Web UI
     - Inherited ``luigi`` central scheduler (basic)
     - Full-featured Dagster UI (Dagit) with asset catalog and run history
   * - Licence
     - Apache 2.0
     - Apache 2.0

``b2luigi`` in a Nutshell
--------------------------

``b2luigi`` extends `luigi <https://luigi.readthedocs.io>`_ with:

* **Batch-system submission** (LSF, HTCondor, SLURM, gbasf2 / LHC grid) — a single flag
  ``--batch`` or ``batch=True`` is enough.
* **Automatic file-path management** — output paths are constructed from task class name and
  parameter values, so you never have to write ``os.path.join(…)`` boilerplate.
* **Parameter hashing** — complex parameters (lists, dicts) can be hashed so that file
  names remain short and filesystem-safe.
* **Settings management** — a hierarchical settings mechanism (per task, global, JSON file)
  replaces hard-coded constants.
* **Run modes** — ``dry-run``, ``show-output``, ``test``, ``remove``, ``remove-only`` without
  any extra tooling.
* **Apptainer/Singularity container support** for HPC environments.

Dagster in a Nutshell
----------------------

Dagster is an *orchestration platform* built around the concept of
**software-defined assets** — persistent data objects (tables, files, ML models) whose
production logic is expressed as Python functions.  Key features include:

* **Asset catalog** with lineage, freshness tracking, and metadata.
* **Dagster UI (Dagit)** — rich real-time dashboard for monitoring runs.
* **Partitions and backfills** — first-class support for processing data in time-window
  slices and re-running historical partitions.
* **Schedules and sensors** — cron-based scheduling and event-driven triggers.
* **Type checking** of op/asset inputs and outputs at runtime using ``DagsterType``.
* **Resources** — abstraction for external services (databases, cloud storage, APIs).
* **Configurable retry policies** and run-level configuration.
* **Daemon** for background scheduling and sensor evaluation.
* **Dagster+ (Cloud)** — hosted option with multi-site deployments and role-based access.
* **Large integration ecosystem** — dbt, Spark, Airflow, Kubernetes, Pandas, Polars, …

Feature-by-Feature Comparison
------------------------------

Workflow Definition
~~~~~~~~~~~~~~~~~~~

In ``b2luigi`` (and ``luigi``) a workflow is a directed acyclic graph (DAG) of *tasks*.
Each task declares its *dependencies* via ``requires()`` and its *outputs* via ``output()``.
The dependency graph is discovered at runtime by traversing these declarations.

.. code-block:: python

    import b2luigi

    class StepA(b2luigi.Task):
        number = b2luigi.IntParameter()

        def output(self):
            return self.add_to_output("result_a.txt")

        def run(self):
            with open(self.get_output_file_name("result_a.txt"), "w") as f:
                f.write(str(self.number * 2))

    class StepB(b2luigi.Task):
        number = b2luigi.IntParameter()

        def requires(self):
            return StepA(number=self.number)

        def output(self):
            return self.add_to_output("result_b.txt")

        def run(self):
            with open(self.get_output_file_name("result_b.txt"), "w") as f:
                f.write("done")

    if __name__ == "__main__":
        b2luigi.process(StepB(number=42), workers=4, batch=True)

In Dagster, the equivalent construct depends on whether you use the **ops/graphs** API
(imperative, similar to functions) or the **assets** API (declarative, data-centric):

.. code-block:: python

    from dagster import asset

    @asset
    def step_a(number: int) -> str:
        return str(number * 2)

    @asset
    def step_b(step_a: str) -> str:
        return "done"

Execution and Scheduling
~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 37 38
   :header-rows: 1

   * - Feature
     - ``b2luigi``
     - Dagster
   * - Local execution
     - ``b2luigi.process(task)``
     - ``dagster dev`` / ``materialize(assets)``
   * - Parallel workers
     - ``workers=N`` argument
     - ``multiprocess_executor`` or ``k8s_job_executor``
   * - Batch/HPC submission
     - ✅ LSF, HTCondor, SLURM, gbasf2, custom
     - ❌ No native HPC batch support (community plugins exist for some clusters)
   * - Cron scheduling
     - ❌ Not built-in (use crontab or ``luigi`` scheduler)
     - ✅ ``@schedule`` decorator with full cron syntax
   * - Event-driven triggers
     - ❌ Not built-in
     - ✅ ``@sensor`` decorator
   * - Daemon required
     - ❌
     - ✅ Required for schedules and sensors
   * - Retry on failure
     - ❌ Restart whole graph (idempotent by design)
     - ✅ Per-op / per-asset retry policy

Observability and Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 37 38
   :header-rows: 1

   * - Feature
     - ``b2luigi``
     - Dagster
   * - Web UI
     - Basic ``luigi`` central scheduler
     - Full Dagster UI (Dagit) with asset catalog
   * - Run history
     - ❌
     - ✅ Persistent run log in SQLite / PostgreSQL
   * - Asset lineage
     - ❌
     - ✅ Visual lineage graph
   * - Metadata emission
     - ❌
     - ✅ ``context.add_output_metadata(…)``
   * - Data freshness / SLAs
     - ❌
     - ✅ ``FreshnessPolicy``
   * - Asset checks
     - ❌
     - ✅ ``@asset_check`` decorator

Parameters and Partitioning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 37 38
   :header-rows: 1

   * - Feature
     - ``b2luigi``
     - Dagster
   * - Typed parameters
     - ✅ ``IntParameter``, ``FloatParameter``, ``ListParameter``, ``DictParameter``, …
     - ✅ ``Config`` classes or ``RunConfig``
   * - Parameter sweeps
     - ✅ Very ergonomic — ``requires()`` yields many instances
     - Possible via partitions; more verbose setup
   * - Partitions (time/category)
     - ❌ No built-in concept
     - ✅ ``DailyPartitionsDefinition``, ``StaticPartitionsDefinition``, …
   * - Backfills
     - ❌
     - ✅ Built-in UI-driven backfills

File and Data Management
~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 37 38
   :header-rows: 1

   * - Feature
     - ``b2luigi``
     - Dagster
   * - Output path generation
     - ✅ Automatic from task name + parameters
     - Manual or via ``IOManager``
   * - Parameter hashing
     - ✅ ``hashed=True`` on any parameter
     - ❌
   * - IOManagers
     - ❌
     - ✅ Pluggable storage backends (S3, GCS, ADLS, …)
   * - Incremental / partial outputs
     - Via ``on_temporary_file`` helper
     - Via ``IOManager``
   * - XRootD targets
     - ✅ Native support for HEP data stores
     - ❌

Environment and Deployment
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 37 38
   :header-rows: 1

   * - Feature
     - ``b2luigi``
     - Dagster
   * - Container support
     - ✅ Apptainer / Singularity per task
     - ✅ Docker / Kubernetes executor
   * - Environment scripts
     - ✅ ``env_script`` setting
     - Via ``EnvVar`` resources or Dockerfile
   * - Multiple code locations
     - ❌
     - ✅ Workspace with many repos/code locations
   * - Cloud hosting
     - ❌
     - ✅ Dagster+ (paid)
   * - Heterogeneous batch queues
     - ✅ Per-task ``batch_system`` setting
     - Partial (ops can specify different executors but no HPC integration)

What Would ``b2luigi`` Need to Match Dagster?
----------------------------------------------

To reach feature parity with Dagster the following additions would be needed.
Note that many of these are deliberate non-goals of ``b2luigi``, which favours
simplicity and HPC-friendliness over general data-engineering features.

1. **Software-defined assets with lineage tracking**
   A data-centric API where tasks declare *what data they produce* rather than
   *what computation they run*.  This would require a new task base class and a
   metadata store.

2. **Persistent run history and audit trail**
   A backend (SQLite or PostgreSQL) that records every run, its parameters, status,
   and timing information accessible via UI and API.

3. **Rich web UI (asset catalog, lineage graph, run explorer)**
   The existing ``luigi`` central scheduler gives a basic task-graph view;
   a Dagit-equivalent would need to be purpose-built.

4. **Built-in cron scheduler and event-driven sensors**
   ``b2luigi`` relies on external cron jobs or manual invocation.  A daemon with
   schedule/sensor support would close this gap.

5. **Partitions and backfills**
   First-class time-window and categorical partitioning with UI-driven backfill
   support.

6. **Per-asset/op retry policies**
   Currently users must re-run the whole graph; automatic per-task retry with
   configurable back-off would improve reliability.

7. **IOManagers / pluggable storage backends**
   An abstraction that decouples *what is stored* from *where it is stored*,
   enabling easy switching between local disk, S3, GCS, etc.

8. **Runtime type checking of inputs/outputs**
   Automatic validation of data types at task boundaries.

9. **Data freshness policies and asset checks**
   Declarative SLA monitoring and built-in data-quality assertions.

10. **Integration marketplace**
    Official connectors to dbt, Spark, Pandas, Polars, Fivetran, etc.

What ``b2luigi`` Can Do That Dagster Cannot
-------------------------------------------

``b2luigi`` has a number of strengths that make it uniquely suited for scientific and
HPC workloads:

1. **Native HPC batch-system submission**
   Out-of-the-box support for LSF, HTCondor, SLURM, and the LHC computing grid
   (via gbasf2).  Dagster has no equivalent; running on HPC clusters with Dagster
   requires significant custom infrastructure.

2. **Automatic parameter-derived file paths**
   Output files are automatically placed at paths that encode all task parameters,
   eliminating boilerplate and making results self-describing on disk.

3. **Massive parameter sweeps with minimal code**
   ``requires()`` can ``yield`` hundreds of dependency instances in a single line,
   making it trivial to fan-out over experiment configurations, energy bins,
   systematic variations, etc.

4. **Single-process scheduling — no daemon needed**
   The entire orchestrator runs in a single Python process.  No background daemon,
   no database, no extra services to maintain.

5. **Apptainer/Singularity support for HPC**
   Per-task container support using Apptainer (formerly Singularity), the standard
   container runtime on HPC clusters where Docker is not permitted.

6. **XRootD target support**
   Native integration with XRootD (xrd/root protocol) for reading and writing to
   data stores common in particle physics.

7. **Grid computing via gbasf2**
   Direct submission to the LHC Worldwide Computing Grid through the gbasf2 wrapper,
   which handles grid credentials, dataset management, and job monitoring.

8. **Lightweight footprint**
   ``b2luigi`` has very few dependencies.  Dagster's full stack (daemon, Dagit,
   event-log database) consumes significant memory and CPU even when idle.

9. **Gradual adoption**
   Because ``b2luigi`` is fully compatible with plain ``luigi``, existing ``luigi``
   pipelines can adopt ``b2luigi`` features incrementally.

Dagster and Heterogeneous Workflows
-------------------------------------

Dagster provides several mechanisms to accommodate *heterogeneous* workflows — pipelines
where different steps have very different resource requirements, execution environments,
or computational characteristics:

* **Multiple executors per job** — individual ops can be tagged to run on different
  executors (multi-process, Celery, Kubernetes).
* **Op-level resource binding** — each op can declare the resources it needs
  (e.g. a GPU-enabled Spark cluster vs. a small Pandas DataFrame operation) and
  Dagster will provision them independently.
* **Code locations** — logically separate services can be defined in different code
  locations, each with its own Python environment and dependencies.
* **Docker / Kubernetes executors** — ops run in isolated containers, so each can use
  a completely different OS image and dependency set.

However, Dagster is **not** designed for the kind of heterogeneity found in HPC
environments:

* There is no native concept of a *batch queue* or *wall-clock time limit*.
* All jobs run on infrastructure that Dagster manages (or that is reachable via
  Kubernetes/Celery).  Submitting to an external scheduler (LSF, SLURM, HTCondor)
  requires significant custom work.
* Mixing many thousands of short, compute-intensive tasks (a common HEP pattern)
  with Dagster's per-run overhead can be impractical.

Common Complaints About Dagster
---------------------------------

The following are recurring themes in the community (GitHub issues, community Slack,
Reddit, blog posts):

1. **Steep learning curve**
   Dagster introduces many new concepts — ops, graphs, jobs, assets, resources,
   IOManagers, partitions, sensors, schedules, code locations.  New users often
   report confusion about which abstraction to use and how they relate to each other.

2. **Paradigm shift from ops to assets**
   The framework underwent a major conceptual shift (from the original "solids/pipelines"
   → "ops/graphs" → "software-defined assets") which broke documentation coherence and
   left many tutorials out of date.

3. **Heavy resource footprint**
   Running Dagster locally requires the Dagit web server and the dagster-daemon process.
   For simple pipelines this overhead is disproportionate.

4. **Complex configuration**
   Configuring resources, IOManagers, and launchers involves a lot of boilerplate YAML
   or Python config classes.  Small mistakes can be hard to diagnose.

5. **Difficult debugging for long-running pipelines**
   When a job fails inside a multi-process or Kubernetes executor the stack trace can
   be hard to retrieve, and replaying individual ops is not always straightforward.

6. **Overkill for simple pipelines**
   Teams with straightforward ETL needs often feel that Dagster introduces more
   ceremony than value compared to simpler tools.

7. **Daemon must stay running**
   For schedules and sensors to fire, ``dagster-daemon`` must be running at all times.
   Managing this process adds operational burden in on-premises deployments.

8. **Cost and lock-in with Dagster+**
   The managed cloud offering (Dagster+) provides the best user experience but
   involves recurring costs and some degree of vendor lock-in.

9. **Limited HPC / batch-queue support**
   Data scientists who come from a scientific-computing background find it difficult to
   integrate Dagster with HPC schedulers (SLURM, LSF, HTCondor) without writing
   custom executors.

Summary
-------

+------------------------------------------+-----------+---------+
| Capability                               | b2luigi   | Dagster |
+==========================================+===========+=========+
| HPC batch submission (LSF/HTCondor/SLURM)| ✅        | ❌      |
+------------------------------------------+-----------+---------+
| LHC Grid (gbasf2)                        | ✅        | ❌      |
+------------------------------------------+-----------+---------+
| Apptainer/Singularity containers         | ✅        | ❌      |
+------------------------------------------+-----------+---------+
| Automatic parameter-derived paths        | ✅        | ❌      |
+------------------------------------------+-----------+---------+
| Parameter sweeps (fan-out)               | ✅        | Partial |
+------------------------------------------+-----------+---------+
| XRootD targets                           | ✅        | ❌      |
+------------------------------------------+-----------+---------+
| No daemon required                       | ✅        | ❌      |
+------------------------------------------+-----------+---------+
| Software-defined assets + lineage        | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Rich web UI (Dagit)                      | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Built-in cron scheduling                 | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Event-driven sensors                     | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Partitions and backfills                 | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Per-task retry policies                  | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Persistent run history                   | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Pluggable IOManagers (S3, GCS, …)        | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Runtime type checking                    | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Data freshness / SLA policies            | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Integration marketplace (dbt, Spark, …)  | ❌        | ✅      |
+------------------------------------------+-----------+---------+
| Cloud hosting option                     | ❌        | ✅      |
+------------------------------------------+-----------+---------+

**Recommendation:**

* Choose ``b2luigi`` if your workflows run on **HPC clusters** (LSF, HTCondor, SLURM,
  LHC grid), if you need **large parameter sweeps**, or if you want a **lightweight**
  orchestrator that stays close to ``luigi`` idioms.
* Choose Dagster if you need a **full data-platform experience** with an asset catalog,
  rich UI, built-in scheduling/sensors, data lineage, and integrations with the modern
  data-engineering ecosystem (dbt, Spark, Airflow, cloud object stores).

The two tools address largely different use cases and should be seen as complementary
rather than competing choices.

.. _Dagster: https://dagster.io
.. _`Dagster documentation`: https://docs.dagster.io
.. _`luigi documentation`: https://luigi.readthedocs.io
