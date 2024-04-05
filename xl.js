const fs = require('fs');
const xlsx = require('xlsx');
const { MongoClient } = require('mongodb');

const inputFile = 'contactInfosample.csv';
const uri = 'mongodb://127.0.0.1:27017';
const client = new MongoClient(uri);

async function splitIntoFilesAndStore(inputFile, chunkSizeForCSV, chunkSizeForDB) {
    try {
        const workbook = xlsx.readFile(inputFile);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const data = xlsx.utils.sheet_to_json(worksheet);

        // Split data into chunks for CSV files
        const chunkDataForCSV = [];
        for (let i = 0; i < Math.ceil(data.length / chunkSizeForCSV); i++) {
            const startIndex = i * chunkSizeForCSV;
            const endIndex = Math.min((i + 1) * chunkSizeForCSV, data.length);
            chunkDataForCSV.push(data.slice(startIndex, endIndex));
        }

        // Write data to CSV files
        for (let i = 0; i < chunkDataForCSV.length; i++) {
            const fileName = `file_${i + 1}.csv`;
            await writeCSVToFile(fileName, chunkDataForCSV[i]);
            console.log(`Chunk ${i + 1} (${chunkDataForCSV[i].length} records) written to file: ${fileName}`);
        }

        // Split data into chunks for database insertion
        const chunkDataForDB = [];
        for (let i = 0; i < Math.ceil(data.length / chunkSizeForDB); i++) {
            const startIndex = i * chunkSizeForDB;
            const endIndex = Math.min((i + 1) * chunkSizeForDB, data.length);
            chunkDataForDB.push(data.slice(startIndex, endIndex));
        }

        // Store data in MongoDB
        for (let i = 0; i < chunkDataForDB.length; i++) {
            await storeInMongoDB(chunkDataForDB[i]);
            console.log(`(${chunkDataForDB[i].length} records) inserted into MongoDB`);
        }

        console.log("All chunks processed successfully.");
    } catch (error) {
        console.error('Error:', error);
    }
}

async function writeCSVToFile(fileName, data) {
    const csvHeader = Object.keys(data[0]).join(',');
    const csvData = [csvHeader, ...data.map(row => Object.values(row).join(','))].join('\n');
    try {
        await fs.promises.writeFile(fileName, csvData);
    } catch (error) {
        throw new Error(`Error writing to file ${fileName}: ${error}`);
    }
}

async function storeInMongoDB(data) {
    try {
        await client.connect();
        console.log('Connected to MongoDB');
        const database = client.db('batch_db');
        const collection = database.collection('orders');
        const result = await collection.insertMany(data);
        return result;
    } catch (error) {
        throw new Error('Error inserting data into MongoDB:', error);
    }
}

const chunkSizeForCSV = 1000;
const chunkSizeForDB = 100;
splitIntoFilesAndStore(inputFile, chunkSizeForCSV, chunkSizeForDB);
