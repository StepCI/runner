import path from 'node:path'
import { JSONPath } from 'jsonpath-plus'
import Ajv from 'ajv'
import parseDuration from 'parse-duration'
import { gRPCRequestMetadata, makeRequest } from 'cool-grpc'
const { co2 } = require('@tgwf/co2')
import {
  StepCheckCaptures,
  StepCheckJSONPath,
  StepCheckMatcher,
  StepCheckPerformance,
} from '..'
import { CapturesStorage } from './../utils/runner'
import { TLSCertificate, getTLSCertificate } from './../utils/auth'
import { Credential } from './../utils/auth'
import { StepRunResult, WorkflowConfig, WorkflowOptions } from '..'
import { Matcher, checkResult } from '../matcher'

export type gRPCStep = {
  proto: string | string[]
  includeDirs: string | string[]
  host: string
  service: string
  method: string
  data?: object | object[]
  timeout?: string | number
  metadata?: gRPCRequestMetadata
  auth?: gRPCStepAuth
  captures?: gRPCStepCaptures
  check?: gRPCStepCheck
}

export type gRPCStepAuth = {
  tls?: Credential['tls']
}

export type gRPCStepCaptures = {
  [key: string]: gRPCStepCapture
}

export type gRPCStepCapture = {
  jsonpath?: string
}

export type gRPCStepCheck = {
  json?: object
  schema?: object
  jsonpath?: StepCheckJSONPath | StepCheckMatcher
  captures?: StepCheckCaptures
  performance?: StepCheckPerformance | StepCheckMatcher
  size?: number | Matcher[]
  co2?: number | Matcher[]
}

export type gRPCStepRequest = {
  proto?: string | string[]
  host: string
  service: string
  method: string
  metadata?: gRPCRequestMetadata
  data?: object | object[]
  tls?: Credential['tls']
  size?: number
}

export type gRPCStepResponse = {
  body: object | object[]
  duration: number
  co2: number
  size: number
  status?: number
  statusText?: string
  metadata?: object
}

export default async function (
  params: gRPCStep,
  captures: CapturesStorage,
  schemaValidator: Ajv,
  options?: WorkflowOptions,
  config?: WorkflowConfig
) {
  const stepResult: StepRunResult = {
    type: 'grpc',
  }

  const ssw = new co2()

  // Load TLS configuration from file or string
  let tlsConfig: TLSCertificate | undefined
  if (params.auth) {
    tlsConfig = await getTLSCertificate(params.auth.tls, {
      workflowPath: options?.path,
    })
  }

  const gatherPaths = function(configPaths: string | string[] | undefined, 
      stepPaths: string | string[] | undefined, 
      optionsPath: string | undefined) : string[] {
    const paths: string[] = [];
    if (configPaths) {
      paths.push(
        ...(Array.isArray(configPaths) ? configPaths : [configPaths])
      )
    }

    if (stepPaths) {
      paths.push(
        ...(Array.isArray(stepPaths) ? stepPaths : [stepPaths])
      )
    }

    return paths.map((p) =>
      path.join(path.dirname(optionsPath || __dirname), p)
    )
  }

  const proto: string[] = gatherPaths(config?.grpc?.proto, params.proto, options?.path)
  const includeDirs: string[] = gatherPaths(config?.grpc?.includeDirs, params.includeDirs, options?.path)

  const request: gRPCStepRequest = {
    proto,
    host: params.host,
    metadata: params.metadata,
    service: params.service,
    method: params.method,
    data: params.data,
  }

  const { metadata, statusCode, statusMessage, data, size } = await makeRequest(
    proto,
    {
      ...request,
      loaderOptions: { includeDirs },
      tls: tlsConfig,
      beforeRequest: (req) => {
        options?.ee?.emit('step:grpc_request', request)
      },
      afterResponse: (res) => {
        options?.ee?.emit('step:grpc_response', res)
      },
      options: {
        deadline: typeof params.timeout === 'string' ? parseDuration(params.timeout) : params.timeout
      }
    }
  )

  stepResult.request = request
  stepResult.response = {
    body: data,
    co2: ssw.perByte(size),
    size: size,
    status: statusCode,
    statusText: statusMessage,
    metadata,
  }

  // Captures
  if (params.captures) {
    for (const name in params.captures) {
      const capture = params.captures[name]
      if (capture.jsonpath) {
        captures[name] = JSONPath({ path: capture.jsonpath, json: data })[0]
      }
    }
  }

  if (params.check) {
    stepResult.checks = {}

    // Check JSON
    if (params.check.json) {
      stepResult.checks.json = checkResult(data, params.check.json)
    }

    // Check Schema
    if (params.check.schema) {
      const validate = schemaValidator.compile(params.check.schema)
      stepResult.checks.schema = {
        expected: params.check.schema,
        given: data,
        passed: validate(data),
      }
    }

    // Check JSONPath
    if (params.check.jsonpath) {
      stepResult.checks.jsonpath = {}

      for (const path in params.check.jsonpath) {
        const result = JSONPath({ path, json: data })
        stepResult.checks.jsonpath[path] = checkResult(
          result[0],
          params.check.jsonpath[path]
        )
      }
    }

    // Check captures
    if (params.check.captures) {
      stepResult.checks.captures = {}

      for (const capture in params.check.captures) {
        stepResult.checks.captures[capture] = checkResult(
          captures[capture],
          params.check.captures[capture]
        )
      }
    }

    // Check performance
    if (params.check.performance) {
      stepResult.checks.performance = {}

      if (params.check.performance.total) {
        stepResult.checks.performance.total = checkResult(
          stepResult.response?.duration,
          params.check.performance.total
        )
      }
    }

    // Check byte size
    if (params.check.size) {
      stepResult.checks.size = checkResult(size, params.check.size)
    }

    // Check co2 emissions
    if (params.check.co2) {
      stepResult.checks.co2 = checkResult(
        stepResult.response?.co2,
        params.check.co2
      )
    }
  }

  return stepResult
}
