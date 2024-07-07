const express = require("express");
const { MongoClient, ServerApiVersion, ObjectId } = require('mongodb');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;

const scraping = require('./modules/scraping'); // Certifique-se de que o caminho está correto
const uri = process.env.URL_MONGO;
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  }
});

client.connect().then(() => {
  const database = client.db("flashstat");
  const collection = database.collection("scraping-data");

  app.get('/extracao/:country/:league/:season/:time', async (req, res) => {
    try {
      const { country, league, season, time } = req.params;
      await scraping.scraping(country, league, season, time, collection);
      res.send('Extração de dados realizada com sucesso');
    } catch (error) {
      console.error(error);
      res.status(500).send('Erro na extração de dados');
    }
  });
  
  app.get('/listar_datasets', async (req, res) => {
    try {
      const datasets = await collection.find().toArray();
      res.json(datasets.map(dataset => ({
        id: dataset._id,
        filename: dataset.filename,
        status: dataset.status,
      })));
    } catch (error) {
      res.status(500).send('Erro ao listar datasets');
    }
  });
  
  app.get('/listar_dados/:id_dataset', async (req, res) => {
    try {
      const { id_dataset } = req.params;
      const dataset = await collection.findOne({ _id: new ObjectId(id_dataset) });
      if (dataset) {
        const csvData = dataset.file.buffer.toString('utf8');
        const [headerLine, ...lines] = csvData.split('\n');
        const headers = headerLine.split(',');

        const jsonData = lines.map(line => {
          const values = line.split(',');
          return headers.reduce((obj, header, index) => {
            obj[header] = values[index];
            return obj;
          }, {});
        });

        res.json(jsonData);
      } else {
        res.status(404).send('Dataset não encontrado');
      }
    } catch (error) {
      res.status(500).send('Erro ao listar dados do dataset');
    }
  });
  
  app.get('/baixar/:id_dataset', async (req, res) => {
    try {
      const { id_dataset } = req.params;
      const dataset = await collection.findOne({ _id: new ObjectId(id_dataset) });
      if (dataset) {
        res.setHeader('Content-Disposition', `attachment; filename=${dataset.filename}`);
        res.setHeader('Content-Type', 'application/octet-stream');
        res.send(dataset.file.buffer);
      } else {
        res.status(404).send('Dataset não encontrado');
      }
    } catch (error) {
      res.status(500).send('Erro ao baixar dataset');
    }
  });
  

  app.listen(PORT, () => {
    console.log(`Server listening on ${PORT}`);
  });
}).catch(error => console.error('Failed to connect to the database', error));
