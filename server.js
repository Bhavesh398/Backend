import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import satelliteRoutes from './routes/satellites.js';
import alertRoutes from './routes/alerts.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.use('/api/satellites', satelliteRoutes);
app.use('/api/alerts', alertRoutes);

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', message: 'Orbit Shield API is running' });
});

app.listen(PORT, () => {
  console.log(`🚀 Orbit Shield Backend running on port ${PORT}`);
});
