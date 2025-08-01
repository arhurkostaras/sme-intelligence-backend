const express = require('express');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// 🚀 SIMPLE TEST ENDPOINT
app.get('/api/test', (req, res) => {
    console.log('Test endpoint called!');
    res.json({
        status: 'success',
        message: 'Server is working perfectly!',
        timestamp: new Date().toISOString(),
        port: PORT
    });
});

// 📊 MARKET INTELLIGENCE API
app.get('/api/market-intelligence', (req, res) => {
    console.log('Market intelligence endpoint called!');
    res.json({
        status: 'success',
        data: {
            marketTrends: [
                'SME growth up 12% in Q3 2025',
                'Technology adoption increasing 18%',
                'Remote work trending across Canada',
                'Digital transformation accelerating',
                'AI integration growing in SMEs'
            ],
            regions: [
                'Ontario: Strong growth (+15%)',
                'BC: Growing tech sector (+12%)',
                'Quebec: Stable manufacturing (+8%)',
                'Alberta: Energy sector recovery (+10%)',
                'Atlantic: Tourism rebound (+14%)'
            ],
            sectors: [
                'Technology & Software',
                'Healthcare & Life Sciences',
                'Professional Services',
                'Manufacturing',
                'Financial Services'
            ],
            keyMetrics: {
                totalActiveSMEs: 1247000,
                newRegistrations: 23450,
                employmentGrowth: '+8.2%',
                averageRevenue: '$2.4M CAD'
            }
        },
        cached: false,
        timestamp: new Date().toISOString()
    });
});

// 📈 ANALYTICS API
app.get('/api/analytics', (req, res) => {
    console.log('Analytics endpoint called!');
    res.json({
        status: 'success',
        data: {
            overview: {
                totalSMEs: 1247000,
                growth: '+8.2%',
                employment: 12500000,
                revenue: '$745B CAD'
            },
            submissions: {
                totalSubmissions: 523,
                weeklySubmissions: 47,
                monthlyGrowth: '+12%',
                averageResponseTime: '2.3 days'
            },
            provinceDistribution: {
                'Ontario': { count: 235, percentage: '45%' },
                'British Columbia': { count: 128, percentage: '24%' },
                'Quebec': { count: 89, percentage: '17%' },
                'Alberta': { count: 71, percentage: '14%' }
            },
            industryTrends: [
                { industry: 'Technology', growth: '+15%', smes: 156000 },
                { industry: 'Healthcare', growth: '+12%', smes: 98000 },
                { industry: 'Professional Services', growth: '+9%', smes: 234000 },
                { industry: 'Manufacturing', growth: '+6%', smes: 187000 }
            ],
            recentActivity: [
                'New CPA certification program launched',
                'SME tax incentive program updated',
                'Digital transformation grants available',
                'Export assistance program expanded'
            ]
        },
        timestamp: new Date().toISOString()
    });
});

// 📝 SME SUBMISSION API
app.post('/api/sme-submission', (req, res) => {
    console.log('SME submission received:', req.body);
    
    // Simple validation
    const { business_name, industry, province } = req.body;
    
    if (!business_name || !industry || !province) {
        return res.status(400).json({
            status: 'error',
            message: 'Missing required fields: business_name, industry, province'
        });
    }
    
    // Mock successful submission
    res.json({
        status: 'success',
        message: 'SME data submitted successfully!',
        submissionId: 'SME-' + Date.now(),
        data: {
            business_name,
            industry,
            province,
            submittedAt: new Date().toISOString(),
            status: 'processing'
        }
    });
});

// 🏠 ROOT ENDPOINT
app.get('/', (req, res) => {
    res.json({
        status: 'success',
        message: '🍁 Canadian SME Intelligence API',
        version: '1.0.0',
        endpoints: [
            'GET  /api/test - Health check',
            'GET  /api/market-intelligence - Market data and trends',
            'GET  /api/analytics - SME analytics and statistics',
            'POST /api/sme-submission - Submit SME data'
        ],
        timestamp: new Date().toISOString()
    });
});

// 🚀 START SERVER
app.listen(PORT, () => {
    console.log(`🚀 Canadian SME Intelligence Server running on port ${PORT}`);
    console.log(`📊 API endpoints available:`);
    console.log(`   GET  /api/test`);
    console.log(`   GET  /api/market-intelligence`);
    console.log(`   GET  /api/analytics`);
    console.log(`   POST /api/sme-submission`);
    console.log(`🍁 Ready to serve Canadian SME intelligence!`);
});
