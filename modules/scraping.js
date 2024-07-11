const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const { ObjectId } = require('mongodb');
const { parse } = require('csv-parse');

async function loadCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(parse({ delimiter: ',' }))
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', (error) => reject(error));
  });
}

async function scraping(country, tournament, season, time, collection) {
    const browser = await puppeteer.launch({
        executablePath: process.env.NODE_ENV === 'production' ? process.env.PUPPETEER_EXECUTABLE_PATH : puppeteer.executablePath(),
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-gpu',
        ]
    });
    const page = await browser.newPage();

    await page.setViewport({ width: 1366, height: 768 });

    const response = await page.goto(`https://www.flashscore.com/football/${country}/${tournament}-${season}/results/`);
    if (response.ok()) {
        const start = Date.now();
        const dataset = await verify_and_get_exists_dataset(collection, country, tournament, season, time);
        if (dataset) {
            const filePath = `modules/temp/${tournament}-${season}-${time}.csv`;
            fs.writeFileSync(filePath, dataset.buffer, 'utf8');
            const csvData = await loadCSV(filePath);
            await get_rounds_missing(csvData, page, country, tournament, season, time, collection);
            fs.unlinkSync(filePath); 
          } else {
            await get_every_rounds_from_tournament(page, country, tournament, season, time, collection);
          }
        const end = Date.now();
        console.log(`Tempo: ${(end-start)/60000} minutos`);
    } else {
        console.log('Erro 404');
    }
    await browser.close();
    console.log("Fechando o browser")
}

async function get_every_rounds_from_tournament(page, country, tournament, season, time, collection) {
    try {
        console.log("Criando dataset novo");
        await page.waitForSelector('a.event__more');
        while (true) {
            await page.waitForSelector('a.event__more');
            await page.click('a.event__more');
            await page.waitForSelector('a.event__more');
        }
    } catch (error) {
        await page.waitForSelector('div.event__match--twoLine');
        console.log("Times carregados com sucesso");
        const matches_data = []
        let match_ids = await page.$$eval('div.event__match--twoLine', elements => elements.map(el => el.getAttribute('id').replace('g_1_', '')));
        let qntd_jogos = match_ids.length;
        if (qntd_jogos > 0) {
            let contador = 0;
            for (match_id of match_ids) {
                try  {
                    matches_data.push(await match_page(page, match_id, time));
                    contador++;
                    console.log(contador + " de " + qntd_jogos + " - " + (contador / qntd_jogos * 100).toFixed(2) + "%");
                } catch (error) {
                    console.log(error)
                }
            }
        }
        saveToCSV(matches_data, tournament, collection, country, tournament, season, time);
    }
}

async function get_rounds_missing(dataset, page, country, tournament, season, time, collection) {
    try {
        console.log("Abrindo todos os jogos");
        await page.waitForSelector('a.event__more');
        while (true) {
            await page.waitForSelector('a.event__more');
            await page.click('a.event__more');
            await page.waitForSelector('a.event__more');
        }
    } catch (error) {
        console.log("Completando jogos");
        let jogos_faltando = [];
        let match_ids = await page.$$eval('div.event__match--twoLine', elements => elements.map(el => el.getAttribute('id').replace('g_1_', '')));
        let qntd_jogos = match_ids.length;
        if (qntd_jogos != dataset.length) {
            for (let match_id of match_ids) {
                if (!dataset.some(row => row[0] === match_id)) {
                    jogos_faltando.push(match_id);
                }
            }
        }
        if (jogos_faltando.length > 0) {
            let contador = 0;
            console.log("Falta os seguintes jogos: " + jogos_faltando);
            for (let id_jogo of jogos_faltando) {
                contador++;
                console.log(contador + " de " + jogos_faltando.length + " - " + (contador / jogos_faltando.length * 100).toFixed(2) + "%");
                const matchData = await match_page(page, id_jogo, time);
                dataset.push(matchData);
            }
            saveToCSV(dataset, tournament, collection, country, tournament, season, time);
        } else {
            console.log("Nenhum jogo faltando");
        }
    }
}

async function match_page(page, match_id, time) {
    await page.goto(`https://www.flashscore.com/match/${match_id}/#/match-summary/match-statistics/${time}`)
    await page.waitForSelector('div._row_1nw75_8');
    const round = await page.$eval('span.tournamentHeader__country', el => {
        const parts = el.innerText.split(' - ');
        return parts[parts.length - 1];
    });
    const homeTeam = await page.$eval('div.duelParticipant__home', el => el.innerText);
    const awayTeam = await page.$eval('div.duelParticipant__away', el => el.innerText);
    const logoHome = await page.$eval('#detail > div.duelParticipant > div.duelParticipant__home > a > img', el => el.src);
    const logoAway = await page.$eval('#detail > div.duelParticipant > div.duelParticipant__away > a > img', el => el.src);
    const score = await page.$eval('div.detailScore__wrapper', el => el.innerText);
    const date_match = await page.$eval('div.duelParticipant__startTime', el => el.innerText.split(' ')[0]);
    const [fthg, ftag] = score.split('\n-\n');
    await page.waitForSelector("div._row_1nw75_8");
    const statistics = (await page.$$('div._row_1nw75_8'));

    const matchData = {
        'Match_ID': match_id,
        'Round': round,
        'Date': date_match,
        'LogoHome': logoHome,
        'LogoAway': logoAway,
        'HomeTeam': homeTeam,
        'AwayTeam': awayTeam,
        'FTHG': fthg,
        'FTAG': ftag,
    };

    for (statistic of statistics) {
        const statistic_name = await statistic.$eval('div._category_1ague_4', el => el.innerText.replace("(xG)", ""));
        const first_letters = statistic_name.split(' ').map(word => word[0]).join('');
        const home_value = await statistic.$eval('div._homeValue_1jbkc_9', el => el.innerText);
        const away_value = await statistic.$eval('div._awayValue_1jbkc_13', el => el.innerText);
        const statistic_name_home = first_letters + 'HT';
        const statistic_name_away = first_letters + 'AT';
        matchData[statistic_name_home] = home_value;
        matchData[statistic_name_away] = away_value;
    }
    return matchData
}

function saveToCSV(dados, nome_arquivo, collection, country, tournament, season, time) {
    let headerPadrao = {
        'Match_ID': '',
        'Round': '',
        'Date': '',
        'LogoHome': '',
        'LogoAway': '',
        'HomeTeam': '',
        'AwayTeam': '',
        'FTHG': '',
        'FTAG': '',
        'EGHT': '',
        'EGAT': '',
        'BPHT': '',
        'BPAT': '',
        'GAHT': '',
        'GAAT': '',
        'SoGHT': '',
        'SoGAT': '',
        'FKHT': '',
        'FKAT': '',
        'CKHT': '',
        'CKAT': '',
        'OHT': '',
        'OAT': '',
        'THT': '',
        'TAT': '',
        'GSHT': '',
        'GSAT': '',
        'FHT': '',
        'FAT': '',
        'RCHT': '',
        'RCAT': '',
        'YCHT': '',
        'YCAT': '',
        'TPHT': '',
        'TPAT': '',
        'AHT': '',
        'AAT': '',
        'DAHT': '',
        'DAAT': '',
        'CCHT': '',
        'CCAT': ''
    };

    // Criar o cabeçalho com base no jogo com mais estatísticas
    const header = Object.keys(headerPadrao).join(',') + '\n';
    // Criar as linhas, adicionando 'N/A' para estatísticas ausentes
    const rows = dados.map(match => {
        return Object.keys(headerPadrao).map(key => {
            return match[key] || '-';
        }).join(',');
    }).join('\n');

    const csvContent = header + rows;
    const filename = `${nome_arquivo}-${season}-${time}.csv`;
    fs.writeFileSync(filename, csvContent, 'utf8');
    // Mover a importação para dentro da função
    data_mongo = {
        '_id': new ObjectId(),
        'filename': `${nome_arquivo}-${season}-${time}.csv`,
        'file': fs.readFileSync(filename),
        'country': country,
        'tournament': tournament,
        'season': season,
        'time': time
    }
    sendToMongo(collection, data_mongo);
    fs.unlinkSync(filename);
}

async function sendToMongo(collection, data) {
    const existingDataset = await collection.findOne({ filename: data.filename });
    if (!existingDataset) {
        const result = await collection.insertOne(data);
    } else {
        const result = await collection.updateOne({ filename: data.filename }, { $set: { file: data.file } });
    }
}

async function get_next_rounds(collection, country, tournament, season) {
    // svg.liveBet 
    // https://www.flashscore.com/football/brazil/serie-a/fixtures/
    const browser = await puppeteer.launch({
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-gpu',
        ]
    });
    const page = await browser.newPage();
    
    console.log("Abrindo Browser")
    await page.setViewport({ width: 1366, height: 768 });
    
    const dados = [];
    const response = await page.goto(`https://www.flashscore.com/football/${country}/${tournament}-${season}/fixtures/`);
    console.log(`https://www.flashscore.com/football/${country}/${tournament}-${season}/fixtures/`)
    if (response.ok()) {
        const eventos = (await page.$$('div.event__match--twoLine'));
        for (evento of eventos) {
            try {
                const liveBetExists = await evento.$('svg.liveBet') !== null;
                if (liveBetExists) {
                    const date = await evento.$eval('div.event__time', el => el.innerText);
                    const logoHome = await evento.$eval('div.event__homeParticipant > img', el => el.src);
                    const logoAway = await evento.$eval('div.event__awayParticipant > img', el => el.src);
                    const home = await evento.$eval('div.event__homeParticipant', el => el.innerText);
                    const away = await evento.$eval('div.event__awayParticipant', el => el.innerText);
                    
                    dados.push({
                        date: date,
                        logoHome: logoHome,
                        logoAway: logoAway,
                        home: home,
                        away: away
                    });
                }
            } catch (error) {
                console.log(error);
            }
        }
        saveNextRounds(dados, tournament, collection, country, tournament, season);
    }
    console.log("Fechando Browser")
    await browser.close();
    return dados;
}

function saveNextRounds(dados, nome_arquivo, collection, country, tournament, season) {
    let headerPadrao = {
        'Match_ID': '',
        'Round': '',
        'Date': '',
        'LogoHome': '',
        'LogoAway': '',
        'HomeTeam': '',
        'AwayTeam': '',
    };

    // Criar o cabeçalho com base no jogo com mais estatísticas
    const header = Object.keys(headerPadrao).join(',') + '\n';
    // Criar as linhas, adicionando 'N/A' para estatísticas ausentes
    const rows = dados.map(match => {
        return Object.keys(headerPadrao).map(key => {
            return match[key] || '-';
        }).join(',');
    }).join('\n');

    const csvContent = header + rows;
    const filename = `${nome_arquivo}-${season}-.csv`;
    fs.writeFileSync(filename, csvContent, 'utf8');
    // Mover a importação para dentro da função
    data_mongo = {
        '_id': new ObjectId(),
        'filename': filename,
        'file': fs.readFileSync(filename),
        'country': country,
        'tournament': tournament,
        'season': season,
    }
    sendToMongo(collection, data_mongo);
    fs.unlinkSync(filename);
}

async function verify_and_get_exists_dataset(collection, country, tournament, season, time) {
    try {
      const result = await collection.findOne({ country, tournament, season, time });
      if (result) {
        return result.file;
      } else {
        return false;
      }
    } catch (error) {
      console.error('Erro ao verificar dataset existente:', error);
      throw error;
    }
}
  

module.exports = {
    scraping,
    get_next_rounds
};
