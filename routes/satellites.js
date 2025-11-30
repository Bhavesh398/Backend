import express from 'express';

const router = express.Router();

// Mock satellite data
const satellites = [
  {
    id: 'Sat-01',
    name: 'Sat-01',
    status: 'Healthy',
    risk: 'safe',
    position: { x: 0, y: 0, z: 3.5 },
    velocity: { x: 0.3, y: 0, z: 0 },
    altitude: 450,
    inclination: 51.6,
    lastUpdate: new Date().toISOString()
  },
  {
    id: 'Sat-02',
    name: 'Sat-02',
    status: 'Medium Risk',
    risk: 'medium',
    position: { x: 0, y: 0, z: 3.8 },
    velocity: { x: 0.25, y: 0, z: 0 },
    altitude: 470,
    inclination: 52.3,
    lastUpdate: new Date().toISOString()
  },
  {
    id: 'Sat-03',
    name: 'Sat-03',
    status: 'High Risk',
    risk: 'high',
    position: { x: 0, y: 0, z: 4.2 },
    velocity: { x: 0.35, y: 0, z: 0 },
    altitude: 500,
    inclination: 53.1,
    lastUpdate: new Date().toISOString()
  },
  {
    id: 'Sat-04',
    name: 'Sat-04',
    status: 'High Risk',
    risk: 'high',
    position: { x: 0, y: 0, z: 4.5 },
    velocity: { x: 0.28, y: 0, z: 0 },
    altitude: 520,
    inclination: 54.0,
    lastUpdate: new Date().toISOString()
  }
];

// Get all satellites
router.get('/', (req, res) => {
  res.json({
    success: true,
    count: satellites.length,
    data: satellites
  });
});

// Get satellite by ID
router.get('/:id', (req, res) => {
  const satellite = satellites.find(sat => sat.id === req.params.id);
  
  if (!satellite) {
    return res.status(404).json({
      success: false,
      message: 'Satellite not found'
    });
  }
  
  res.json({
    success: true,
    data: satellite
  });
});

// Get collision risk data
router.get('/risk/timeline', (req, res) => {
  const hours = 24;
  const timelineData = [];
  
  for (let i = 0; i <= hours; i++) {
    timelineData.push({
      hour: i,
      riskLevel: Math.random() * 100,
      timestamp: new Date(Date.now() + i * 3600000).toISOString()
    });
  }
  
  res.json({
    success: true,
    data: timelineData
  });
});

export default router;
