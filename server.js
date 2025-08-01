// server.js - Real-Time SME Intelligence Backend
// Complete Node.js server for Canadian SME intelligence data collection
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const cheerio = require('cheerio');
const { Client } = require('pg');
const Redis = require('redis');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Database Configuration
const dbClient = new Client({
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 5432,
    database: process.env.DB_NAME || 'sme_intelligence',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD
});

// Redis Configuration for Caching
const redisClient = Redis.createClient({
    url: process.env.REDIS_PRIVATE_URL || process.env.REDIS_URL || 'redis://localhost:6379'
});

// Initialize Database Connection
async function initializeDatabase() {
    try {
        await dbClient.connect();
        console.log('✅ PostgreSQL connected successfully');
        
        // Create tables if they don't exist
        await createTables();
        console.log('✅ Database tables initialized');
    } catch (error) {
        console.error('❌ Database connection failed:', error);
    }
}

// Create Database Tables
async function createTables() {
    const createTablesQuery = `
        CREATE TABLE IF NOT EXISTS market_data (
            id SERIAL PRIMARY KEY,
            source VARCHAR(100) NOT NULL,
            metric_name VARCHAR(200) NOT NULL,
            metric_value DECIMAL(10,2),
            province VARCHAR(50),
            industry VARCHAR(100),
            collection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_current BOOLEAN DEFAULT true
        );

        CREATE TABLE IF NOT EXISTS sme_submissions (
            id SERIAL PRIMARY KEY,
            business_name VARCHAR(200),
            industry VARCHAR(100),
            province VARCHAR(50),
            revenue_range VARCHAR(50),
            employees_range VARCHAR(50),
            primary_challenge TEXT,
            seasonal_peak VARCHAR(20),
            tech_stack VARCHAR(100),
            growth_stage VARCHAR(50),
            submission_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS cpa_performance (
            id SERIAL PRIMARY KEY,
            cpa_name VARCHAR(200),
            province VARCHAR(50),
            specialization VARCHAR(100),
            response_time_hours DECIMAL(4,2),
            satisfaction_rating DECIMAL(3,2),
            matches_completed INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_market_data_current ON market_data(is_current);
        CREATE INDEX IF NOT EXISTS idx_submissions_date ON sme_submissions(submission_date);
        CREATE INDEX IF NOT EXISTS idx_cpa_province ON cpa_performance(province);
    `;
    
    await dbClient.query(createTablesQuery);
}

// 🇨🇦 STATISTICS CANADA API INTEGRATION
class StatisticsCanadaAPI {
    constructor() {
        this.baseUrl = 'https://www150.statcan.gc.ca/t1/wds/rest';
    }

    async getAccountingServicesPriceIndex() {
        try {
            // Statistics Canada Table: 18-10-0005-01 (Services Producer Price Indexes)
            const response = await axios.get(`${this.baseUrl}/getFullTableDataByPid/1810000501`);
            
            console.log('📊 Statistics Canada - Accounting Services data retrieved');
            return this.parseStatCanData(response.data, 'accounting_services_index');
        } catch (error) {
            console.error('❌ Statistics Canada API error:', error.message);
            return null;
        }
    }

    async getAdvancedTechnologySurvey() {
        try {
            // Statistics Canada Advanced Technology Survey data
            const response = await axios.get(`${this.baseUrl}/getChangedSurveyData/5555`);
            
            console.log('🔬 Statistics Canada - Advanced Technology Survey retrieved');
            return this.parseStatCanData(response.data, 'technology_adoption');
        } catch (error) {
            console.error('❌ Advanced Technology Survey error:', error.message);
            return null;
        }
    }

    parseStatCanData(data, metricType) {
        // Parse Statistics Canada JSON format
        const parsedData = [];
        
        if (data && data.object && data.object.dimension) {
            const observations = data.object.observation;
            const dimensions = data.object.dimension;
            
            for (const [key, value] of Object.entries(observations)) {
                if (value && value.value) {
                    parsedData.push({
                        metric_type: metricType,
                        value: parseFloat(value.value),
                        period: key,
                        source: 'Statistics Canada'
                    });
                }
            }
        }
        
        return parsedData;
    }
}

// 🏢 ISED CANADA INTEGRATION
class ISEDCanadaAPI {
    constructor() {
        this.baseUrl = 'https://www.ic.gc.ca/eic/site/061.nsf';
    }

    async getSMEInnovationData() {
        try {
            // ISED SME Profile data scraping (they don't have public API)
            const response = await axios.get('https://www.ic.gc.ca/eic/site/061.nsf/eng/h_03018.html');
            const $ = cheerio.load(response.data);
            
            const smeData = [];
            
            // Extract SME statistics from ISED pages
            $('.data-table tr').each((index, element) => {
                const row = $(element);
                const metric = row.find('td:first-child').text().trim();
                const value = row.find('td:nth-child(2)').text().trim();
                
                if (metric && value) {
                    smeData.push({
                        source: 'ISED Canada',
                        metric_name: metric,
                        metric_value: this.parseValue(value),
                        collection_date: new Date()
                    });
                }
            });

            console.log('🏢 ISED Canada - SME Innovation data retrieved');
            return smeData;
        } catch (error) {
            console.error('❌ ISED Canada scraping error:', error.message);
            return [];
        }
    }

    parseValue(valueString) {
        // Extract numeric values from text
        const numericValue = valueString.match(/[\d,]+\.?\d*/);
        return numericValue ? parseFloat(numericValue[0].replace(/,/g, '')) : null;
    }
}

// 📊 INDUSTRY REPORT SCRAPERS
class IndustryReportScraper {
    async getCWBankResearch() {
        try {
            const response = await axios.get('https://www.cwbank.ca/en/resources/small-business', {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (compatible; SME-Intelligence-Bot/1.0)'
                }
            });
            
            const $ = cheerio.load(response.data);
            const insights = [];
            
            // Look for cash flow statistics
            $('.research-finding').each((index, element) => {
                const finding = $(element).text();
                if (finding.includes('cash flow') || finding.includes('SME')) {
                    insights.push({
                        source: 'CW Bank Research',
                        finding: finding.trim(),
                        collection_date: new Date()
                    });
                }
            });

            console.log('🏦 CW Bank - SME Research data retrieved');
            return insights;
        } catch (error) {
            console.error('❌ CW Bank scraping error:', error.message);
            return [];
        }
    }

    async getRobertHalfSalaryData() {
        try {
            const response = await axios.get('https://www.roberthalf.ca/en/salary-guide', {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (compatible; SME-Intelligence-Bot/1.0)'
                }
            });
            
            const $ = cheerio.load(response.data);
            const salaryData = [];
            
            // Extract CPA salary and demand data
            $('.salary-data').each((index, element) => {
                const role = $(element).find('.role-title').text();
                const salary = $(element).find('.salary-range').text();
                
                if (role.includes('CPA') || role.includes('Accountant')) {
                    salaryData.push({
                        source: 'Robert Half',
                        role: role.trim(),
                        salary_range: salary.trim(),
                        collection_date: new Date()
                    });
                }
            });

            console.log('💼 Robert Half - CPA Salary data retrieved');
            return salaryData;
        } catch (error) {
            console.error('❌ Robert Half scraping error:', error.message);
            return [];
        }
    }
}

// 🔄 DATA COLLECTION ORCHESTRATOR
class DataCollectionOrchestrator {
    constructor() {
        this.statCanAPI = new StatisticsCanadaAPI();
        this.isedAPI = new ISEDCanadaAPI();
        this.industryScraper = new IndustryReportScraper();
    }

    async collectAllData() {
        console.log('🚀 Starting comprehensive data collection...');
        
        try {
            // Collect from all sources simultaneously
            const [
                statCanAccounting,
                statCanTech,
                isedSME,
                cwBankData,
                robertHalfData
            ] = await Promise.all([
                this.statCanAPI.getAccountingServicesPriceIndex(),
                this.statCanAPI.getAdvancedTechnologySurvey(),
                this.isedAPI.getSMEInnovationData(),
                this.industryScraper.getCWBankResearch(),
                this.industryScraper.getRobertHalfSalaryData()
            ]);

            // Store data in database
            await this.storeMarketData([
                ...statCanAccounting || [],
                ...statCanTech || [],
                ...isedSME || [],
                ...cwBankData || [],
                ...robertHalfData || []
            ]);

            console.log('✅ Data collection completed successfully');
            
            // Update cache
            await this.updateCache();
            
        } catch (error) {
            console.error('❌ Data collection failed:', error);
        }
    }

    async storeMarketData(dataArray) {
        // Mark previous data as not current
        await dbClient.query('UPDATE market_data SET is_current = false WHERE is_current = true');
        
        // Insert new data
        for (const data of dataArray) {
            await dbClient.query(
                'INSERT INTO market_data (source, metric_name, metric_value, province, industry) VALUES ($1, $2, $3, $4, $5)',
                [data.source, data.metric_name || data.finding, data.metric_value, data.province, data.industry]
            );
        }
    }

    async updateCache() {
        // Cache frequently accessed data in Redis
        const recentData = await dbClient.query(
            'SELECT * FROM market_data WHERE is_current = true ORDER BY collection_date DESC'
        );
        
        await redisClient.setex('latest_market_data', 3600, JSON.stringify(recentData.rows));
    }
}
// Simple test endpoint - NO database calls
app.get('/api/test', (req, res) => {
    console.log('Test endpoint called!');
    res.json({
        status: 'success',
        message: 'Server is working!',
        timestamp: new Date().toISOString(),
        port: process.env.PORT || 'not set'
    });
});

// 📈 API ENDPOINTS
const dataOrchestrator = new DataCollectionOrchestrator();

// Get latest market intelligence
// Simplified market intelligence endpoint - NO database calls
app.get('/api/market-intelligence', async (req, res) => {
    console.log('Market intelligence endpoint called!');
    res.json({
        status: 'success',
        data: {
            marketTrends: ['SME growth up 12%', 'Tech adoption increasing', 'Remote work trending'],
            regions: ['Ontario: Strong', 'BC: Growing', 'Quebec: Stable'],
            sectors: ['Technology', 'Healthcare', 'Professional Services']
        },
        cached: false,
        timestamp: new Date().toISOString()
    });
});
        }

        // Fallback to database
        const result = await dbClient.query(
            'SELECT * FROM market_data WHERE is_current = true ORDER BY collection_date DESC'
        );

        res.json({
            status: 'success',
            data: result.rows,
            cached: false,
            timestamp: new Date()
        });
    } catch (error) {
        res.status(500).json({ status: 'error', message: error.message });
    }
});

// Submit SME data from your forms
app.post('/api/sme-submission', async (req, res) => {
    try {
        const {
            business_name, industry, province, revenue_range, 
            employees_range, primary_challenge, seasonal_peak, 
            tech_stack, growth_stage
        } = req.body;

        await dbClient.query(
            `INSERT INTO sme_submissions 
             (business_name, industry, province, revenue_range, employees_range, 
              primary_challenge, seasonal_peak, tech_stack, growth_stage) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
            [business_name, industry, province, revenue_range, employees_range,
             primary_challenge, seasonal_peak, tech_stack, growth_stage]
        );

        res.json({ status: 'success', message: 'SME data recorded successfully' });
    } catch (error) {
        res.status(500).json({ status: 'error', message: error.message });
    }
});

// Get real-time analytics
// Simplified analytics endpoint - NO database calls  
app.get('/api/analytics', async (req, res) => {
    console.log('Analytics endpoint called!');
    res.json({
        status: 'success',
        data: {
            totalSMEs: 1247000,
            growth: '+8.2%',
            employment: 12500000,
            revenue: '$745B CAD',
            totalSubmissions: 523,
            weeklySubmissions: 47,
            provinceDistribution: {
                'Ontario': 235,
                'BC': 128,
                'Quebec': 89,
                'Alberta': 71
            },
            industryTrends: ['Technology', 'Healthcare', 'Professional Services']
        },
        timestamp: new Date().toISOString()
    });
});
    } catch (error) {
        res.status(500).json({ status: 'error', message: error.message });
    }
});

// 🕐 AUTOMATED DATA COLLECTION SCHEDULE
// Every day at 6 AM collect fresh data
cron.schedule('0 6 * * *', async () => {
    console.log('⏰ Starting scheduled data collection...');
    await dataOrchestrator.collectAllData();
});

// 🚀 START SERVER
async function startServer() {
   // TEMPORARILY DISABLE DATABASE CONNECTIONS
// await initializeDatabase();
// await redisClient.connect();

console.log('🚀 Starting server without databases (temporary)...');
    
    // Start HTTP server FIRST
    app.listen(PORT, () => {
        console.log(`🚀 Real-Time SME Intelligence Server running on port ${PORT}`);
        console.log(`📊 API endpoints:`);
        console.log(`   GET  /api/market-intelligence`);
        console.log(`   POST /api/sme-submission`);
        console.log(`   GET  /api/analytics`);
    });
    
    // Then do data collection in background
    console.log('🔄 Running initial data collection...');
    dataOrchestrator.collectAllData().catch(console.error);  // Don't wait for it
}

startServer().catch(console.error);
