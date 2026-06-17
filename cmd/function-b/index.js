const express = require('express');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const PORT = process.env.FUNCTION_B_PORT || 8083;
const FUNCTION_C_URL = process.env.FUNCTION_C_URL || 'http://function-c:8084/notify';

app.use((req, res, next) => {
  const start = Date.now();
  const requestId = req.headers['x-request-id'] || 'unknown';

  res.on('finish', () => {
    const duration = Date.now() - start;
    console.log(`[FunctionB:Node.js] request_id=${requestId} path=${req.path} status=${res.statusCode} duration=${duration}ms`);
  });

  next();
});

app.get('/health', (req, res) => {
  res.json({
    service: 'function-b',
    language: 'node.js',
    status: 'healthy',
    time: new Date().toISOString()
  });
});

app.post('/payment', async (req, res) => {
  const requestId = req.headers['x-request-id'] || 'unknown';
  const traceId = req.headers['x-trace-id'] || uuidv4();
  const parentSpanId = req.headers['x-parent-span-id'] || '';
  const spanId = uuidv4();
  const startTime = Date.now();

  try {
    const { order_id, amount, user_id, product_id } = req.body;

    console.log(`[FunctionB] request_id=${requestId} processing payment: order=${order_id} amount=${amount} user=${user_id}`);

    if (!order_id || !amount || !user_id) {
      return res.status(400).json({ error: 'Missing required fields: order_id, amount, user_id' });
    }

    await new Promise(resolve => setTimeout(resolve, 80 + Math.random() * 120));

    const paymentId = `PAY-${uuidv4().slice(0, 8)}`;
    const transactionId = `TXN-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const paymentStatus = Math.random() > 0.02 ? 'success' : 'failed';

    if (paymentStatus === 'success') {
      const notifyReq = {
        order_id: order_id,
        payment_id: paymentId,
        user_id: user_id,
        amount: amount,
        status: paymentStatus,
        notification_type: 'payment_confirmation'
      };

      try {
        await axios.post(FUNCTION_C_URL, notifyReq, {
          headers: {
            'Content-Type': 'application/json',
            'X-Request-ID': requestId,
            'X-Trace-ID': traceId,
            'X-Parent-Span-ID': spanId
          },
          timeout: 10000
        });
        console.log(`[FunctionB] request_id=${requestId} notification sent to FunctionC`);
      } catch (notifyErr) {
        console.log(`[FunctionB] request_id=${requestId} notification call failed (non-critical): ${notifyErr.message}`);
      }
    }

    const duration = Date.now() - startTime;
    console.log(`[FunctionB] request_id=${requestId} payment completed: payment_id=${paymentId} status=${paymentStatus} duration=${duration}ms`);

    res.json({
      payment_id: paymentId,
      order_id: order_id,
      status: paymentStatus,
      amount: amount,
      transaction_id: transactionId
    });
  } catch (error) {
    const duration = Date.now() - startTime;
    console.error(`[FunctionB] request_id=${requestId} error: ${error.message}, duration=${duration}ms`);
    res.status(500).json({ error: `Payment processing failed: ${error.message}` });
  }
});

app.listen(PORT, () => {
  console.log(`FunctionB (Node.js - Payment Service) starting on port ${PORT}`);
});
