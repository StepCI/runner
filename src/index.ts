import { CookieJar, Cookie } from 'tough-cookie'
import { renderObject as liquidlessRenderObject } from 'liquidless'
import { fake } from 'liquidless-faker'
import { naughtystring } from 'liquidless-naughtystrings'
import { EventEmitter } from 'node:events'
import fs from 'fs'
import yaml from 'js-yaml'
import $RefParser from '@apidevtools/json-schema-ref-parser'
import Ajv from 'ajv'
import addFormats from 'ajv-formats'
import pLimit from 'p-limit'
import path from 'node:path'
import { Phase } from 'phasic'
import { Matcher, CheckResult, CheckResults } from './matcher'
import { LoadTestCheck } from './loadtesting'
import { parseCSV, TestData } from './utils/testdata'
import { CapturesStorage, checkCondition, didChecksPass } from './utils/runner'
import { CredentialsStorage } from './utils/auth'
import runHTTPStep, { HTTPStep, HTTPStepRequest, HTTPStepResponse } from './steps/http'
import runGRPCStep, { gRPCStep, gRPCStepRequest, gRPCStepResponse } from './steps/grpc'
import runSSEStep, { SSEStep, SSEStepRequest, SSEStepResponse } from './steps/sse'
import runDelayStep from './steps/delay'
import runPluginStep, { PluginStep } from './steps/plugin'
import runTRPCStep, { tRPCStep } from './steps/trpc'
import runGraphQLStep, { GraphQLStep } from './steps/graphql'
import parseDuration from 'parse-duration'
import { addCustomSchemas } from './utils/schema'

export type Workflow = {
  version: string
  name: string
  env?: WorkflowEnv
  /**
   * @deprecated Import files using `$refs` instead.
  */
  include?: string[]
  before?: Test
  tests: Tests
  after?: Test
  components?: WorkflowComponents
  config?: WorkflowConfig
}

export type WorkflowEnv = {
  [key: string]: string
}

export type WorkflowComponents = {
  schemas?: {
    [key: string]: any
  }
  credentials?: CredentialsStorage
}

export type WorkflowConfig = {
  loadTest?: {
    phases: Phase[]
    check?: LoadTestCheck
  },
  continueOnFail?: boolean,
  http?: {
    baseURL?: string
    rejectUnauthorized?: boolean
    http2?: boolean
  }
  grpc?: {
    proto: string | string[]
    includeDirs: string | string[]
  }
  concurrency?: number
}

export type WorkflowOptions = {
  path?: string
  secrets?: WorkflowOptionsSecrets
  ee?: EventEmitter
  env?: WorkflowEnv
  concurrency?: number
}

type WorkflowOptionsSecrets = {
  [key: string]: string
}

export type WorkflowResult = {
  workflow: Workflow
  result: {
    tests: TestResult[]
    passed: boolean
    timestamp: Date
    duration: number
    bytesSent: number
    bytesReceived: number
    co2: number
  }
  path?: string
}

export type Test = {
  name?: string
  env?: object
  steps: Step[]
  testdata?: TestData
}

export type Tests = {
  [key: string]: Test
}

export type Step = {
  id?: string
  name?: string
  retries?: {
    count: number
    interval?: string | number
  }
  if?: string
  http?: HTTPStep
  trpc?: tRPCStep
  graphql?: GraphQLStep
  grpc?: gRPCStep
  sse?: SSEStep
  delay?: string
  plugin?: PluginStep
}

export type StepCheckValue = {
  [key: string]: string
}

export type StepCheckJSONPath = {
  [key: string]: any
}

export type StepCheckPerformance = {
  [key: string]: number
}

export type StepCheckCaptures = {
  [key: string]: any
}

export type StepCheckMatcher = {
  [key: string]: Matcher[]
}

export type TestResult = {
  id: string
  name?: string
  steps: StepResult[]
  passed: boolean
  timestamp: Date
  duration: number
  co2: number
  bytesSent: number
  bytesReceived: number
}

export type StepResult = {
  id?: string
  testId: string
  name?: string
  retries?: number
  captures?: CapturesStorage
  cookies?: Cookie.Serialized[]
  errored: boolean
  errorMessage?: string
  passed: boolean
  skipped: boolean
  timestamp: Date
  responseTime: number
  duration: number
  co2: number
  bytesSent: number
  bytesReceived: number
} & StepRunResult

export type StepRunResult = {
  type?: string
  checks?: StepCheckResult
  request?: HTTPStepRequest | gRPCStepRequest | SSEStepRequest | any
  response?: HTTPStepResponse | gRPCStepResponse | SSEStepResponse | any
}

export type StepCheckResult = {
  [key: string]: CheckResult | CheckResults
}

const templateDelimiters = ['${{', '}}']

function renderObject<T extends object>(
  object: object,
  props: object,
): T {
  return liquidlessRenderObject(object, props, {
    filters: {
      fake,
      naughtystring
    },
    delimiters: templateDelimiters
  })
}

// Run from test file
export async function runFromYAML(yamlString: string, options?: WorkflowOptions): Promise<WorkflowResult> {
  const workflow = yaml.load(yamlString)
  const dereffed = await $RefParser.dereference(workflow as any, {
    dereference: {
      circular: 'ignore'
    }
  }) as unknown as Workflow
  return run(dereffed, options)
}

// Run from test file
export async function runFromFile(path: string, options?: WorkflowOptions): Promise<WorkflowResult> {
  const testFile = await fs.promises.readFile(path)
  return runFromYAML(testFile.toString(), { ...options, path })
}

// Run workflow
export async function run(workflow: Workflow, options?: WorkflowOptions): Promise<WorkflowResult> {
  const timestamp = new Date()
  const schemaValidator = new Ajv({ strictSchema: false })
  addFormats(schemaValidator)

  // Templating for env, components, config
  let env = { ...workflow.env, ...options?.env }
  if (workflow.env) {
    env = renderObject(env, { env, secrets: options?.secrets })
  }

  if (workflow.components) {
    workflow.components = renderObject(workflow.components, { env, secrets: options?.secrets })
  }

  if (workflow.components?.schemas) {
    addCustomSchemas(schemaValidator, workflow.components.schemas)
  }

  if (workflow.config) {
    workflow.config = renderObject(workflow.config, { env, secrets: options?.secrets })
  }

  if (workflow.include) {
    for (const workflowPath of workflow.include) {
      const testFile = await fs.promises.readFile(path.join(path.dirname(options?.path || __dirname), workflowPath))
      const test = yaml.load(testFile.toString()) as Workflow
      workflow.tests = { ...workflow.tests, ...test.tests }
    }
  }

  const concurrency = options?.concurrency || workflow.config?.concurrency || Object.keys(workflow.tests).length
  const limit = pLimit(concurrency <= 0 ? 1 : concurrency)

  const testResults: TestResult[] = []
  const captures: CapturesStorage = {}

  // Run `before` section
  if (workflow.before) {
    const beforeResult = await runTest('before', workflow.before, schemaValidator, options, workflow.config, env, captures)
    testResults.push(beforeResult)
  }

  // Run `tests` section
  const input: Promise<TestResult>[] = []
  Object.entries(workflow.tests).map(([id, test]) => input.push(limit(() => runTest(id, test, schemaValidator, options, workflow.config, env, { ...captures }))))
  testResults.push(...await Promise.all(input))

  // Run `after` section
  if (workflow.after) {
    const afterResult = await runTest('after', workflow.after, schemaValidator, options, workflow.config, env, captures)
    testResults.push(afterResult)
  }

  const workflowResult: WorkflowResult = {
    workflow,
    result: {
      tests: testResults,
      timestamp,
      passed: testResults.every(test => test.passed),
      duration: Date.now() - timestamp.valueOf(),
      co2: testResults.map(test => test.co2).reduce((a, b) => a + b),
      bytesSent: testResults.map(test => test.bytesSent).reduce((a, b) => a + b),
      bytesReceived: testResults.map(test => test.bytesReceived).reduce((a, b) => a + b),
    },
    path: options?.path
  }

  options?.ee?.emit('workflow:result', workflowResult)
  return workflowResult
}

async function runTest(id: string, test: Test, schemaValidator: Ajv, options?: WorkflowOptions, config?: WorkflowConfig, env?: object, capturesStorage?: CapturesStorage): Promise<TestResult> {
  const testResult: TestResult = {
    id,
    name: test.name,
    steps: [],
    passed: true,
    timestamp: new Date(),
    duration: 0,
    co2: 0,
    bytesSent: 0,
    bytesReceived: 0
  }

  const captures: CapturesStorage = capturesStorage ?? {}
  const cookies = new CookieJar()
  let previous: StepResult | undefined
  let testData: object = {}

  // Load test data
  if (test.testdata) {
    const parsedCSV = await parseCSV(test.testdata, { ...test.testdata.options, workflowPath: options?.path })
    testData = parsedCSV[Math.floor(Math.random() * parsedCSV.length)]
  }

  for (let step of test.steps) {
    const tryStep = async () => runStep(previous, step, id, test, captures, cookies, schemaValidator, testData, options, config, env)
    let stepResult = await tryStep()

    // Retries
    if ((stepResult.errored || (!stepResult.passed && !stepResult.skipped)) && step.retries && step.retries.count > 0) {
      for (let i = 0; i < step.retries.count; i++) {
        await new Promise(resolve => {
          setTimeout(resolve, typeof step.retries?.interval === 'string' ? parseDuration(step.retries?.interval) : step.retries?.interval)
        })

        stepResult = await tryStep()
        if (stepResult.passed) break
      }
    }

    testResult.steps.push(stepResult)
    previous = stepResult
    options?.ee?.emit('step:result', stepResult)
  }

  testResult.duration = Date.now() - testResult.timestamp.valueOf()
  testResult.co2 = testResult.steps.map(step => step.co2).reduce((a, b) => a + b)
  testResult.bytesSent = testResult.steps.map(step => step.bytesSent).reduce((a, b) => a + b)
  testResult.bytesReceived = testResult.steps.map(step => step.bytesReceived).reduce((a, b) => a + b)
  testResult.passed = testResult.steps.every(step => step.passed)

  options?.ee?.emit('test:result', testResult)
  return testResult
}

async function runStep (previous: StepResult | undefined, step: Step, id: string, test: Test, captures: CapturesStorage, cookies: CookieJar, schemaValidator: Ajv, testData: object, options?: WorkflowOptions, config?: WorkflowConfig, env?: object) {
  let stepResult: StepResult = {
    id: step.id,
    testId: id,
    name: step.name,
    timestamp: new Date(),
    passed: true,
    errored: false,
    skipped: false,
    duration: 0,
    responseTime: 0,
    bytesSent: 0,
    bytesReceived: 0,
    co2: 0
  }

  let runResult: StepRunResult | undefined

  // Skip current step is the previous one failed or condition was unmet
  if (!config?.continueOnFail && (previous && !previous.passed)) {
    stepResult.passed = false
    stepResult.errorMessage = 'Step was skipped because previous one failed'
    stepResult.skipped = true
  } else if (step.if && !checkCondition(step.if, { captures, env: { ...env, ...test.env } })) {
    stepResult.skipped = true
    stepResult.errorMessage = 'Step was skipped because the condition was unmet'
  } else {
    try {
      step = renderObject(step, {
        captures,
        env: { ...env, ...test.env },
        secrets: options?.secrets,
        testdata: testData
      })

      if (step.http) {
        runResult = await runHTTPStep(step.http, captures, cookies, schemaValidator, options, config)
      }

      if (step.trpc) {
        runResult = await runTRPCStep(step.trpc, captures, cookies, schemaValidator, options, config)
      }

      if (step.graphql) {
        runResult = await runGraphQLStep(step.graphql, captures, cookies, schemaValidator, options, config)
      }

      if (step.grpc) {
        runResult = await runGRPCStep(step.grpc, captures, schemaValidator, options, config)
      }

      if (step.sse) {
        runResult = await runSSEStep(step.sse, captures, schemaValidator, options, config)
      }

      if (step.delay) {
        runResult = await runDelayStep(step.delay)
      }

      if (step.plugin) {
        runResult = await runPluginStep(step.plugin, captures, cookies, schemaValidator, options, config)
      }

      stepResult.passed = didChecksPass(runResult?.checks)
    } catch (error) {
      stepResult.passed = false
      stepResult.errored = true
      stepResult.errorMessage = (error as Error).message
      options?.ee?.emit('step:error', error)
    }
  }

  stepResult.type = runResult?.type
  stepResult.request = runResult?.request
  stepResult.response = runResult?.response
  stepResult.checks = runResult?.checks
  stepResult.responseTime = runResult?.response?.duration || 0
  stepResult.co2 = runResult?.response?.co2 || 0
  stepResult.bytesSent = runResult?.request?.size || 0
  stepResult.bytesReceived = runResult?.response?.size || 0
  stepResult.duration = Date.now() - stepResult.timestamp.valueOf()
  stepResult.captures = Object.keys(captures).length > 0 ? captures : undefined
  stepResult.cookies = Object.keys(cookies.toJSON().cookies).length > 0 ? cookies.toJSON().cookies : undefined
  return stepResult
}
