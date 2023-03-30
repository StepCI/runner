import { gotSsrf as got } from 'got-ssrf';
import { makeRequest } from 'cool-grpc';
import { CookieJar } from 'tough-cookie';
import { renderObject } from 'liquidless';
import { fake } from 'liquidless-faker';
import { naughtystring } from 'liquidless-naughtystrings';
import xpath from 'xpath';
import FormData from 'form-data';
import * as cheerio from 'cheerio';
import { JSONPath } from 'jsonpath-plus';
import { DOMParser } from '@xmldom/xmldom';
import crypto from 'crypto';
import fs from 'fs';
import yaml from 'js-yaml';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { Agent } from 'node:https';
import path from 'node:path';
import { co2 } from '@tgwf/co2';
import { checkResult } from './matcher.js';
import { parseCSV } from './utils/testdata.js';
import { checkCondition, getCookie, didChecksPass } from './utils/runner.js';
import { getAuthHeader, getClientCertificate, getTLSCertificate } from './utils/auth.js';
import { tryFile } from './utils/files.js';
import { addCustomSchemas } from './utils/schema.js';
const templateDelimiters = ['${{', '}}'];
// Run from YAML string
export function runFromYAML(yamlString, options) {
    return run(yaml.load(yamlString), options);
}
// Run from test file
export async function runFromFile(path, options) {
    const testFile = await fs.promises.readFile(path);
    const config = yaml.load(testFile.toString());
    return run(config, { ...options, path });
}
// Run workflow
export async function run(workflow, options) {
    const timestamp = new Date();
    const schemaValidator = new Ajv({ strictSchema: false });
    addFormats(schemaValidator);
    // Add schemas to schema Validator
    if (workflow.components?.schemas) {
        addCustomSchemas(schemaValidator, workflow.components.schemas);
    }
    const env = { ...workflow.env ?? {}, ...options?.env ?? {} };
    let credentials;
    if (workflow.components?.credentials) {
        credentials = renderObject(workflow.components?.credentials, { env }, { delimiters: templateDelimiters });
    }
    let tests = { ...workflow.tests ?? {} };
    if (workflow.include) {
        for (const workflowPath of workflow.include) {
            const testFile = await fs.promises.readFile(path.join(path.dirname(options?.path || __dirname), workflowPath));
            const test = yaml.load(testFile.toString());
            tests = { ...tests, ...test.tests };
        }
    }
    const testResults = await Promise.all(Object.entries(tests).map(([id, test]) => runTest(id, test, schemaValidator, options, workflow.config, env, credentials)));
    const workflowResult = {
        workflow,
        result: {
            tests: testResults,
            timestamp,
            passed: testResults.every(test => test.passed),
            duration: Date.now() - timestamp.valueOf(),
            co2: testResults.map(test => test.co2).reduce((a, b) => a + b)
        },
        path: options?.path
    };
    options?.ee?.emit('workflow:result', workflowResult);
    return workflowResult;
}
async function runTest(id, test, schemaValidator, options, config, env, credentials) {
    const testResult = {
        id,
        name: test.name,
        steps: [],
        passed: true,
        timestamp: new Date(),
        duration: 0,
        co2: 0
    };
    const captures = {};
    const cookies = new CookieJar();
    const ssw = new co2();
    let previous;
    let testData = {};
    // Load test data
    if (test.testdata) {
        const parsedCSV = await parseCSV(test.testdata, { ...test.testdata.options, workflowPath: options?.path });
        testData = parsedCSV[Math.floor(Math.random() * parsedCSV.length)];
    }
    for (let step of test.steps) {
        const stepResult = {
            id: step.id,
            testId: id,
            name: step.name,
            timestamp: new Date(),
            passed: true,
            errored: false,
            skipped: false,
            duration: 0,
            responseTime: 0,
            co2: 0
        };
        // Skip current step is the previous one failed or condition was unmet
        if (((test.config?.continueOnFail ?? true) || (config?.continueOnFail ?? true)) && (previous && !previous.passed)) {
            stepResult.passed = false;
            stepResult.errorMessage = 'Step was skipped because previous one failed';
            stepResult.skipped = true;
        }
        else if (step.if && !checkCondition(step.if, { captures, env: { ...env, ...test.env } })) {
            stepResult.skipped = true;
        }
        else {
            try {
                step = renderObject(step, {
                    captures,
                    env: { ...env, ...test.env },
                    secrets: options?.secrets,
                    testdata: testData
                }, {
                    filters: {
                        fake,
                        naughtystring
                    },
                    delimiters: templateDelimiters
                });
                if (step.http) {
                    stepResult.type = 'http';
                    let requestBody;
                    // Prefix URL
                    if (config?.http?.baseURL) {
                        try {
                            new URL(step.http.url);
                        }
                        catch {
                            step.http.url = config.http.baseURL + step.http.url;
                        }
                    }
                    // Body
                    if (step.http.body) {
                        requestBody = await tryFile(step.http.body, { workflowPath: options?.path });
                    }
                    //  JSON
                    if (step.http.json) {
                        requestBody = JSON.stringify(step.http.json);
                    }
                    // GraphQL
                    if (step.http.graphql) {
                        step.http.method = 'POST';
                        if (!step.http.headers)
                            step.http.headers = {};
                        step.http.headers['Content-Type'] = 'application/json';
                        requestBody = JSON.stringify(step.http.graphql);
                    }
                    // tRPC
                    if (step.http.trpc) {
                        if (step.http.trpc.query) {
                            step.http.method = 'GET';
                            // tRPC Batch queries
                            if (Array.isArray(step.http.trpc.query)) {
                                const payload = step.http.trpc.query.map(e => {
                                    return {
                                        op: Object.keys(e)[0],
                                        data: Object.values(e)[0]
                                    };
                                });
                                const procedures = payload.map(p => p.op).join(',');
                                step.http.url = step.http.url + '/' + procedures.replaceAll('/', '.');
                                step.http.params = {
                                    batch: '1',
                                    input: JSON.stringify(Object.assign({}, payload.map(p => p.data)))
                                };
                            }
                            else {
                                const [procedure, data] = Object.entries(step.http.trpc.query)[0];
                                step.http.url = step.http.url + '/' + procedure.replaceAll('/', '.');
                                step.http.params = {
                                    input: JSON.stringify(data)
                                };
                            }
                        }
                        if (step.http.trpc.mutation) {
                            const [procedure, data] = Object.entries(step.http.trpc.mutation)[0];
                            step.http.method = 'POST';
                            step.http.url = step.http.url + '/' + procedure;
                            requestBody = JSON.stringify(data);
                        }
                    }
                    // Form Data
                    if (step.http.form) {
                        const formData = new URLSearchParams();
                        for (const field in step.http.form) {
                            formData.append(field, step.http.form[field]);
                        }
                        requestBody = formData.toString();
                    }
                    // Multipart Form Data
                    if (step.http.formData) {
                        const formData = new FormData();
                        for (const field in step.http.formData) {
                            if (typeof step.http.formData[field] === 'string') {
                                formData.append(field, step.http.formData[field]);
                            }
                            if (step.http.formData[field].file) {
                                const file = await fs.promises.readFile(path.join(path.dirname(options?.path || __dirname), step.http.formData[field].file));
                                formData.append(field, file);
                            }
                        }
                        requestBody = formData;
                    }
                    // Auth
                    let clientCredentials;
                    if (step.http.auth) {
                        const authHeader = await getAuthHeader(step.http.auth, credentials);
                        if (authHeader) {
                            if (!step.http.headers)
                                step.http.headers = {};
                            step.http.headers['Authorization'] = authHeader;
                        }
                        clientCredentials = await getClientCertificate(step.http.auth, credentials, { workflowPath: options?.path });
                    }
                    // Set Cookies
                    if (step.http.cookies) {
                        for (const cookie in step.http.cookies) {
                            await cookies.setCookie(cookie + '=' + step.http.cookies[cookie], step.http.url);
                        }
                    }
                    // Make a request
                    let sslCertificate;
                    let agent = new Agent({
                        maxCachedSessions: 0
                    });
                    agent.on('keylog', (line, tlsSocket) => {
                        sslCertificate = tlsSocket.getPeerCertificate();
                    });
                    const res = await got(step.http.url, {
                        method: step.http.method,
                        headers: { ...step.http.headers },
                        body: requestBody,
                        searchParams: step.http.params ? new URLSearchParams(step.http.params) : undefined,
                        throwHttpErrors: false,
                        followRedirect: step.http.followRedirects ?? true,
                        timeout: { request: step.http.timeout },
                        retry: { limit: step.http.retries ?? 0 },
                        cookieJar: cookies,
                        http2: config?.http?.http2 ?? false,
                        https: {
                            ...clientCredentials,
                            rejectUnauthorized: config?.http?.rejectUnauthorized ?? true
                        },
                        agent: {
                            https: agent
                        }
                    })
                        .on('request', request => options?.ee?.emit('step:http_request', request))
                        .on('response', response => options?.ee?.emit('step:http_response', response));
                    const responseData = res.rawBody;
                    const body = await new TextDecoder().decode(responseData);
                    stepResult.request = {
                        protocol: 'HTTP/1.1',
                        url: res.url,
                        method: step.http.method,
                        headers: step.http.headers,
                        body: requestBody
                    };
                    stepResult.response = {
                        protocol: `HTTP/${res.httpVersion}`,
                        status: res.statusCode,
                        statusText: res.statusMessage,
                        duration: res.timings.phases.total,
                        headers: res.headers,
                        contentType: res.headers['content-type']?.split(';')[0],
                        timings: res.timings,
                        body: responseData,
                        co2: ssw.perByte(responseData.byteLength)
                    };
                    if (sslCertificate) {
                        stepResult.response.ssl = {
                            valid: new Date(sslCertificate.valid_to) > new Date(),
                            signed: sslCertificate.issuer.CN !== sslCertificate.subject.CN,
                            validUntil: new Date(sslCertificate.valid_to),
                            daysUntilExpiration: Math.round(Math.abs(new Date().valueOf() - new Date(sslCertificate.valid_to).valueOf()) / (24 * 60 * 60 * 1000))
                        };
                    }
                    // Captures
                    if (step.http.captures) {
                        for (const name in step.http.captures) {
                            const capture = step.http.captures[name];
                            if (capture.jsonpath) {
                                const json = JSON.parse(body);
                                captures[name] = JSONPath({ path: capture.jsonpath, json })[0];
                            }
                            if (capture.xpath) {
                                const dom = new DOMParser().parseFromString(body);
                                const result = xpath.select(capture.xpath, dom);
                                captures[name] = result.length > 0 ? result[0].firstChild.data : undefined;
                            }
                            if (capture.header) {
                                captures[name] = res.headers[capture.header];
                            }
                            if (capture.selector) {
                                const dom = cheerio.load(body);
                                captures[name] = dom(capture.selector).html();
                            }
                            if (capture.cookie) {
                                captures[name] = getCookie(cookies, capture.cookie, res.url);
                            }
                            if (capture.regex) {
                                captures[name] = body.match(capture.regex)?.[1];
                            }
                            if (capture.body) {
                                captures[name] = body;
                            }
                        }
                    }
                    if (step.http.check) {
                        stepResult.checks = {};
                        // Check headers
                        if (step.http.check.headers) {
                            stepResult.checks.headers = {};
                            for (const header in step.http.check.headers) {
                                stepResult.checks.headers[header] = checkResult(res.headers[header.toLowerCase()], step.http.check.headers[header]);
                            }
                        }
                        // Check body
                        if (step.http.check.body) {
                            stepResult.checks.body = checkResult(body.trim(), step.http.check.body);
                        }
                        // Check JSON
                        if (step.http.check.json) {
                            const json = JSON.parse(body);
                            stepResult.checks.json = checkResult(json, step.http.check.json);
                        }
                        // Check Schema
                        if (step.http.check.schema) {
                            let sample = body;
                            if (res.headers['content-type']?.includes('json')) {
                                sample = JSON.parse(body);
                            }
                            const validate = schemaValidator.compile(step.http.check.schema);
                            stepResult.checks.schema = {
                                expected: step.http.check.schema,
                                given: sample,
                                passed: validate(sample)
                            };
                        }
                        // Check JSONPath
                        if (step.http.check.jsonpath) {
                            const json = JSON.parse(body);
                            stepResult.checks.jsonpath = {};
                            for (const path in step.http.check.jsonpath) {
                                const result = JSONPath({ path, json });
                                stepResult.checks.jsonpath[path] = checkResult(result[0], step.http.check.jsonpath[path]);
                            }
                        }
                        // Check XPath
                        if (step.http.check.xpath) {
                            stepResult.checks.xpath = {};
                            for (const path in step.http.check.xpath) {
                                const dom = new DOMParser().parseFromString(body);
                                const result = xpath.select(path, dom);
                                stepResult.checks.xpath[path] = checkResult(result.length > 0 ? result[0].firstChild.data : undefined, step.http.check.xpath[path]);
                            }
                        }
                        // Check HTML5 Selectors
                        if (step.http.check.selectors) {
                            stepResult.checks.selectors = {};
                            const dom = cheerio.load(body);
                            for (const selector in step.http.check.selectors) {
                                const result = dom(selector).html();
                                stepResult.checks.selectors[selector] = checkResult(result, step.http.check.selectors[selector]);
                            }
                        }
                        // Check Cookies
                        if (step.http.check.cookies) {
                            stepResult.checks.cookies = {};
                            for (const cookie in step.http.check.cookies) {
                                const value = getCookie(cookies, cookie, res.url);
                                stepResult.checks.cookies[cookie] = checkResult(value, step.http.check.cookies[cookie]);
                            }
                        }
                        // Check captures
                        if (step.http.check.captures) {
                            stepResult.checks.captures = {};
                            for (const capture in step.http.check.captures) {
                                stepResult.checks.captures[capture] = checkResult(captures[capture], step.http.check.captures[capture]);
                            }
                        }
                        // Check status
                        if (step.http.check.status) {
                            stepResult.checks.status = checkResult(res.statusCode, step.http.check.status);
                        }
                        // Check statusText
                        if (step.http.check.statusText) {
                            stepResult.checks.statusText = checkResult(res.statusMessage, step.http.check.statusText);
                        }
                        // Check whether request was redirected
                        if ('redirected' in step.http.check) {
                            stepResult.checks.redirected = checkResult(res.redirectUrls.length > 0, step.http.check.redirected);
                        }
                        // Check redirects
                        if (step.http.check.redirects) {
                            stepResult.checks.redirects = checkResult(res.redirectUrls, step.http.check.redirects);
                        }
                        // Check sha256
                        if (step.http.check.sha256) {
                            const hash = crypto.createHash('sha256').update(Buffer.from(responseData)).digest('hex');
                            stepResult.checks.sha256 = checkResult(hash, step.http.check.sha256);
                        }
                        // Check md5
                        if (step.http.check.md5) {
                            const hash = crypto.createHash('md5').update(Buffer.from(responseData)).digest('hex');
                            stepResult.checks.md5 = checkResult(hash, step.http.check.md5);
                        }
                        // Check Performance
                        if (step.http.check.performance) {
                            stepResult.checks.performance = {};
                            for (const metric in step.http.check.performance) {
                                stepResult.checks.performance[metric] = checkResult(res.timings.phases[metric], step.http.check.performance[metric]);
                            }
                        }
                        // Check SSL certs
                        if (step.http.check.ssl && sslCertificate) {
                            stepResult.checks.ssl = {};
                            if ('valid' in step.http.check.ssl) {
                                stepResult.checks.ssl.valid = checkResult(stepResult.response.ssl?.valid, step.http.check.ssl.valid);
                            }
                            if ('signed' in step.http.check.ssl) {
                                stepResult.checks.ssl.signed = checkResult(stepResult.response.ssl?.signed, step.http.check.ssl.signed);
                            }
                            if (step.http.check.ssl.daysUntilExpiration) {
                                stepResult.checks.ssl.daysUntilExpiration = checkResult(stepResult.response.ssl?.daysUntilExpiration, step.http.check.ssl.daysUntilExpiration);
                            }
                        }
                        // Check byte size
                        if (step.http.check.size) {
                            stepResult.checks.size = checkResult(responseData.byteLength, step.http.check.size);
                        }
                        // Check co2 emissions
                        if (step.http.check.co2) {
                            stepResult.checks.co2 = checkResult(stepResult.response.co2, step.http.check.co2);
                        }
                    }
                }
                if (step.grpc) {
                    stepResult.type = 'grpc';
                    // Load TLS configuration from file or string
                    let tlsConfig;
                    if (step.grpc.auth) {
                        tlsConfig = await getTLSCertificate(step.grpc.auth, credentials, { workflowPath: options?.path });
                    }
                    const request = {
                        proto: Array.isArray(step.grpc.proto)
                            ? step.grpc.proto.map(proto => path.join(path.dirname(options?.path || __dirname), proto))
                            : path.join(path.dirname(options?.path || __dirname), step.grpc.proto),
                        host: step.grpc.host,
                        metadata: step.grpc.metadata,
                        service: step.grpc.service,
                        method: step.grpc.method,
                        data: step.grpc.data
                    };
                    const { data, size } = await makeRequest(step.grpc.proto, {
                        ...request,
                        tls: tlsConfig,
                        beforeRequest: (req) => {
                            stepResult.request = request;
                            options?.ee?.emit('step:grpc_request', request);
                        },
                        afterResponse: (res) => {
                            stepResult.response = {
                                body: res.data,
                                duration: Date.now() - stepResult.timestamp.valueOf(),
                                co2: ssw.perByte(res.size)
                            };
                            options?.ee?.emit('step:grpc_response', res);
                        }
                    });
                    // Captures
                    if (step.grpc.captures) {
                        for (const name in step.grpc.captures) {
                            const capture = step.grpc.captures[name];
                            if (capture.jsonpath) {
                                captures[name] = JSONPath({ path: capture.jsonpath, json: data })[0];
                            }
                        }
                    }
                    if (step.grpc.check) {
                        stepResult.checks = {};
                        // Check JSON
                        if (step.grpc.check.json) {
                            stepResult.checks.json = checkResult(data, step.grpc.check.json);
                        }
                        // Check Schema
                        if (step.grpc.check.schema) {
                            const validate = schemaValidator.compile(step.grpc.check.schema);
                            stepResult.checks.schema = {
                                expected: step.grpc.check.schema,
                                given: data,
                                passed: validate(data)
                            };
                        }
                        // Check JSONPath
                        if (step.grpc.check.jsonpath) {
                            stepResult.checks.jsonpath = {};
                            for (const path in step.grpc.check.jsonpath) {
                                const result = JSONPath({ path, json: data });
                                stepResult.checks.jsonpath[path] = checkResult(result[0], step.grpc.check.jsonpath[path]);
                            }
                        }
                        // Check captures
                        if (step.grpc.check.captures) {
                            stepResult.checks.captures = {};
                            for (const capture in step.grpc.check.captures) {
                                stepResult.checks.captures[capture] = checkResult(captures[capture], step.grpc.check.captures[capture]);
                            }
                        }
                        // Check performance
                        if (step.grpc.check.performance) {
                            stepResult.checks.performance = {};
                            if (step.grpc.check.performance.total) {
                                stepResult.checks.performance.total = checkResult(stepResult.response?.duration, step.grpc.check.performance.total);
                            }
                        }
                        // Check byte size
                        if (step.grpc.check.size) {
                            stepResult.checks.size = checkResult(size, step.grpc.check.size);
                        }
                        // Check co2 emissions
                        if (step.grpc.check.co2) {
                            stepResult.checks.co2 = checkResult(stepResult.response?.co2, step.grpc.check.co2);
                        }
                    }
                }
                stepResult.passed = didChecksPass(stepResult);
            }
            catch (error) {
                stepResult.passed = false;
                stepResult.errored = true;
                stepResult.errorMessage = error.message;
                options?.ee?.emit('step:error', error);
            }
        }
        stepResult.duration = Date.now() - stepResult.timestamp.valueOf();
        stepResult.responseTime = stepResult.response?.duration || 0;
        stepResult.co2 = stepResult.response?.co2 || 0;
        testResult.steps.push(stepResult);
        previous = stepResult;
        options?.ee?.emit('step:result', stepResult);
    }
    testResult.duration = Date.now() - testResult.timestamp.valueOf();
    testResult.co2 = testResult.steps.map(step => step.co2).reduce((a, b) => a + b);
    testResult.passed = testResult.steps.every(step => step.passed);
    options?.ee?.emit('test:result', testResult);
    return testResult;
}
