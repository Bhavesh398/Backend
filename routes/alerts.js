import express from 'express';

const router = express.Router();

// Mock alerts data
const alerts = [
  {
    id: 'alert-001',
    type: 'collision',
    severity: 'high',
    title: 'HIGH RISK WARNING!',
    message: 'Potential collision predicted in 18 minutes.',
    satelliteId: 'Sat-03',
    details: {
      deltaV: '0.21 m/s',
      pathChange: '+3° orbital altitude',
      timeToCollision: 18,
      probability: 0.87
    },
    timestamp: new Date().toISOString(),
    acknowledged: false
  },
  {
    id: 'alert-002',
    type: 'debris',
    severity: 'medium',
    title: 'Space Debris Alert',
    message: 'Debris detected in orbital path of Sat-02.',
    satelliteId: 'Sat-02',
    details: {
      deltaV: '0.15 m/s',
      pathChange: '+1.5° orbital altitude',
      timeToCollision: 45,
      probability: 0.54
    },
    timestamp: new Date(Date.now() - 300000).toISOString(),
    acknowledged: false
  }
];

// Get all alerts
router.get('/', (req, res) => {
  const { severity, acknowledged } = req.query;
  
  let filteredAlerts = [...alerts];
  
  if (severity) {
    filteredAlerts = filteredAlerts.filter(alert => alert.severity === severity);
  }
  
  if (acknowledged !== undefined) {
    const ackValue = acknowledged === 'true';
    filteredAlerts = filteredAlerts.filter(alert => alert.acknowledged === ackValue);
  }
  
  res.json({
    success: true,
    count: filteredAlerts.length,
    data: filteredAlerts
  });
});

// Get alert by ID
router.get('/:id', (req, res) => {
  const alert = alerts.find(a => a.id === req.params.id);
  
  if (!alert) {
    return res.status(404).json({
      success: false,
      message: 'Alert not found'
    });
  }
  
  res.json({
    success: true,
    data: alert
  });
});

// Acknowledge alert
router.patch('/:id/acknowledge', (req, res) => {
  const alert = alerts.find(a => a.id === req.params.id);
  
  if (!alert) {
    return res.status(404).json({
      success: false,
      message: 'Alert not found'
    });
  }
  
  alert.acknowledged = true;
  
  res.json({
    success: true,
    message: 'Alert acknowledged',
    data: alert
  });
});

export default router;
