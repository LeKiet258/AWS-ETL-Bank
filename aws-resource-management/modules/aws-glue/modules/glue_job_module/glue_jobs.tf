locals {
  glue_job_list = var.glue_job_list
}

resource "aws_glue_job" "python" {
  count                     = length(local.glue_job_list.*.GlueJobName)
  name                      = format("%s-glue-jb-%s", var.name_prefix, element(local.glue_job_list.*.GlueJobName, count.index))
  role_arn                  = var.role_arn

  glue_version              = local.glue_job_list[count.index]["Type"] == "glueetl" ? local.glue_job_list[count.index]["GlueVersion"] : null
  worker_type               = local.glue_job_list[count.index]["Type"] == "glueetl" ? lookup(local.glue_job_list[count.index], "WorkerType", "G.1X") : null
  number_of_workers         = local.glue_job_list[count.index]["Type"] == "glueetl" ? lookup(local.glue_job_list[count.index], "NumberOfWorkers", 2) : null
  max_capacity              = local.glue_job_list[count.index]["Type"] == "pythonshell" ? local.glue_job_list[count.index]["MaxCapacity"] : null

  execution_property {
    max_concurrent_runs = element(local.glue_job_list.*.MaxConcurrentRuns, count.index)
  }

  command {
    name                    = element(local.glue_job_list.*.Type, count.index)
    script_location         = format("s3://%s/glue_jobs/%s/%s", var.code_artifact_bucket, var.namespace, element(local.glue_job_list.*.GlueScriptFileName, count.index))
    python_version          = element(local.glue_job_list.*.PythonVersion, count.index)
  }

  default_arguments = merge(
    {
      "--TempDir"         = "s3://${var.temporary_bucket}/glue_temporary"
      "--job-language"    = element(local.glue_job_list.*.JobLanguage, count.index)
      "--extra-py-files"  = format("s3://%s/glue_libs/%s", var.code_artifact_bucket, element(local.glue_job_list.*.JobLibs, count.index))
      "--additional-python-modules"        = length(trimspace(element(local.glue_job_list.*.JobPipLibs, count.index))) > 0 ? join(",", formatlist("s3://%s/pip_pkg/%s", var.code_artifact_bucket, split(",", element(local.glue_job_list.*.JobPipLibs, count.index)))) : null
      "--extra-jars"                       = length(trimspace(element(local.glue_job_list.*.ExtraJars, count.index))) > 0 ? join(",", formatlist("s3://%s/pip_pkg/%s", var.code_artifact_bucket, split(",", element(local.glue_job_list.*.ExtraJars, count.index)))) : null
    }
  )

  tags = var.tags
}