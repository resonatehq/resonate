import express, { Express } from 'express';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { Resource } from '@opentelemetry/resources';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';

import { Resonate } from "../../lib/resonate";
import { DurablePromiseStore } from "../../lib/stores/durable"; // eslint-disable-line @typescript-eslint/no-unused-vars
import * as f from './src/fun';

// Open Telemetry Setup

const sdk = new NodeSDK({
    resource: new Resource({
        [SemanticResourceAttributes.SERVICE_NAME]: 'example/web',
      }),
    traceExporter: new OTLPTraceExporter()
});
sdk.start();

// Express Setup

const app: Express = express();
app.use(express.json());

// Resonate Setup
const resonate = new Resonate();
// const resonate = new Resonate(new DurablePromiseStore("http://localhost:8001"));

resonate.registerModule(f);

app.post('/:name/:id', async (req, res) => {
    
    const { name, id } = req.params;

    const args = req.body;

    try {
        const result = await resonate.run(name, id, args);
        res.status(200).json(result);
    } catch (error) {
        if (error instanceof Error) {
            res.status(500).json(error.message);
        }
        else {
            res.status(500).json(error);
        }
    }

});

app.post('/invoke/:name/:id', async (req, res) => {
    
    const { name, id } = req.params;

    const args = req.body;

    try {
        resonate.run(name, id, args);
        res.status(200).json(`/return/${name}/${id}`);
    } catch (error) {
        if (error instanceof Error) {
            res.status(500).json(error.message);
        }
        else {
            res.status(500).json(error);
        }
    }

});

app.post('/return/:name/:id', async (req, res) => {
    
    const { id } = req.params;

    try {
        const promise = await resonate.get(id);
        if (promise) {
            res.status(200).json({state: promise.state, value: promise.value});
        } else {
            res.status(404).json({error: `Promise ${id} not found`});
        }
    } catch (error) {
        if (error instanceof Error) {
            res.status(500).json(error.message);
        }
        else {
            res.status(500).json(error);
        }
    }

});

app.listen(8080, () => console.log(`Server running on port 8080`));
