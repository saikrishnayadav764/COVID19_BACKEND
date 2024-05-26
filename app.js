const express = require("express");
const sqlite3 = require("sqlite3");
const { open } = require("sqlite");
const path = require("path");
const app = express();
const cors = require('cors');
app.use(express.json());
app.use(cors())
const dbPath = path.join(__dirname, "covid19India.db");
let database = null;
const initializeDbAndServer = async () => {
  try {
    database = await open({ filename: dbPath, driver: sqlite3.Database });
    app.listen(3000, () => {
      console.log("Server is running on http://localhost:3000");
    });
  } catch (error) {
    console.log(`Database Error is ${error}`);
    process.exit(1);
  }
};
initializeDbAndServer();

//API 1
//Returns a list of all states in the state table
const convertStateDbObjectAPI1 = (objectItem) => {
  return {
    stateId: objectItem.state_id,
    stateName: objectItem.state_name,
    population: objectItem.population,
  };
};
app.get("/states/", async (request, response) => {
  const getStatesListQuery = `select * from state;`;
  const getStatesListQueryResponse = await database.all(getStatesListQuery);
  response.send(
    getStatesListQueryResponse.map((eachState) =>
      convertStateDbObjectAPI1(eachState)
    )
  );
});

//API 2
//Returns a state based on the state ID

app.get("/states/:stateId/", async (request, response) => {
  const { stateId } = request.params;
  const getStatesListByIdQuery = `select * from state where state_id = ${stateId};`;
  const getStatesListByIdQueryResponse = await database.get(
    getStatesListByIdQuery
  );
  response.send(convertStateDbObjectAPI1(getStatesListByIdQueryResponse));
});

//API 3
//Creates a district in the district table, district_id is auto-incremented

app.post("/districts/", async (request, response) => {
  const { districtName, stateId, cases, cured, active, deaths } = request.body;
  const createDistrictQuery = `insert into district(district_name,state_id,cases,cured,active,deaths)
    values('${districtName}',${stateId},${cases},${cured},${active},${deaths});`;
  const createDistrictQueryResponse = await database.run(createDistrictQuery);
  response.send("District Successfully Added");
});

//API 4
//Returns a district based on the district ID
const convertDbObjectAPI4 = (objectItem) => {
  return {
    districtId: objectItem.district_id,
    districtName: objectItem.district_name,
    stateId: objectItem.state_id,
    cases: objectItem.cases,
    cured: objectItem.cured,
    active: objectItem.active,
    deaths: objectItem.deaths,
  };
};
app.get("/districts/:districtId/", async (request, response) => {
  const { districtId } = request.params;
  const getDistrictByIdQuery = `select * from district where district_id=${districtId};`;
  const getDistrictByIdQueryResponse = await database.get(getDistrictByIdQuery);
  response.send(convertDbObjectAPI4(getDistrictByIdQueryResponse));
});

//API 5
// Deletes a district from the district table based on the district ID
app.delete("/districts/:districtId/", async (request, response) => {
  const { districtId } = request.params;
  const deleteDistrictQuery = `delete from district where district_id=${districtId};`;
  const deleteDistrictQueryResponse = await database.run(deleteDistrictQuery);
  response.send("District Removed");
});

//API 6
//Updates the details of a specific district based on the district ID
app.put("/districts/:districtId/", async (request, response) => {
  const { districtId } = request.params;
  const { districtName, stateId, cases, cured, active, deaths } = request.body;
  const updateDistrictQuery = `update district set
    district_name = '${districtName}',
    state_id = ${stateId},
    cases = ${cases},
    cured = ${cured},
    active = ${active},
    deaths = ${deaths} where district_id = ${districtId};`;

  const updateDistrictQueryResponse = await database.run(updateDistrictQuery);
  response.send("District Details Updated");
});

//API 7
//Returns the statistics of total cases, cured, active, deaths of a specific state based on state ID
app.get("/states/:stateId/stats/", async (request, response) => {
  const { stateId } = request.params;
  const getStateByIDStatsQuery = `select sum(cases) as totalCases, sum(cured) as totalCured,
    sum(active) as totalActive , sum(deaths) as totalDeaths from district where state_id = ${stateId};`;

  const getStateByIDStatsQueryResponse = await database.get(
    getStateByIDStatsQuery
  );
  response.send(getStateByIDStatsQueryResponse);
});

//API 8
// Returns an object containing the state name of a district based on the district ID
app.get("/districts/:districtId/details/", async (request, response) => {
  const { districtId } = request.params;
  const getDistrictIdQuery = `select state_id from district where district_id = ${districtId};`;
  const getDistrictIdQueryResponse = await database.get(getDistrictIdQuery);
  //console.log(typeof getDistrictIdQueryResponse.state_id);
  const getStateNameQuery = `select state_name as stateName from state where 
  state_id = ${getDistrictIdQueryResponse.state_id}`;
  const getStateNameQueryResponse = await database.get(getStateNameQuery);
  response.send(getStateNameQueryResponse);
});

// API 11: Get the top 5 districts with the highest number of cases
app.get("/top-5-districts-with-highest-cases/", async (request, response) => {
  const getTop5DistrictsWithHighestCasesQuery = `
    SELECT 
      district.district_name AS districtName,
      state.state_name AS stateName,
      district.cases
    FROM 
      district
    JOIN 
      state
    ON 
      district.state_id = state.state_id
    ORDER BY 
      district.cases DESC
    LIMIT 5;
  `;
  const top5DistrictsWithHighestCases = await database.all(getTop5DistrictsWithHighestCasesQuery);
  response.send(top5DistrictsWithHighestCases);
});

// API 10: Get the state with the highest number of active cases
app.get("/state-with-highest-active-cases/", async (request, response) => {
  const getStateWithHighestActiveCasesQuery = `
    SELECT 
      state.state_name AS stateName,
      SUM(district.active) AS totalActiveCases
    FROM 
      state
    JOIN 
      district
    ON 
      state.state_id = district.state_id
    GROUP BY 
      state.state_name
    ORDER BY 
      totalActiveCases DESC
    LIMIT 1;
  `;
  const stateWithHighestActiveCases = await database.get(getStateWithHighestActiveCasesQuery);
  response.send(stateWithHighestActiveCases);
});


// API 9: Get a list of all districts with their state names
app.get("/districts-with-state-names/", async (request, response) => {
  const getDistrictsWithStateNamesQuery = `
    SELECT 
      district.district_id AS districtId,
      district.district_name AS districtName,
      state.state_name AS stateName,
      district.cases,
      district.cured,
      district.active,
      district.deaths
    FROM 
      district
    JOIN 
      state
    ON 
      district.state_id = state.state_id;
  `;
  const districtsWithStateNames = await database.all(getDistrictsWithStateNamesQuery);
  response.send(districtsWithStateNames);
});


// API 12: Get the total number of cases, cured, active, and deaths in the entire country
app.get("/total-stats-country/", async (request, response) => {
  const getTotalStatsCountryQuery = `
    SELECT 
      SUM(cases) AS totalCases,
      SUM(cured) AS totalCured,
      SUM(active) AS totalActive,
      SUM(deaths) AS totalDeaths
    FROM 
      district;
  `;
  const totalStatsCountry = await database.get(getTotalStatsCountryQuery);
  response.send(totalStatsCountry);
});


// API 13: Get the number of districts in each state
app.get("/districts-count-by-state/", async (request, response) => {
  const getDistrictsCountByStateQuery = `
    SELECT 
      state.state_name AS stateName,
      COUNT(district.district_id) AS numberOfDistricts
    FROM 
      state
    JOIN 
      district
    ON 
      state.state_id = district.state_id
    GROUP BY 
      state.state_name;
  `;
  const districtsCountByState = await database.all(getDistrictsCountByStateQuery);
  response.send(districtsCountByState);
});


// API 14: Get the state with the lowest number of deaths
app.get("/state-with-lowest-deaths/", async (request, response) => {
  const getStateWithLowestDeathsQuery = `
    SELECT 
      state.state_name AS stateName,
      SUM(district.deaths) AS totalDeaths
    FROM 
      state
    JOIN 
      district
    ON 
      state.state_id = district.state_id
    GROUP BY 
      state.state_name
    ORDER BY 
      totalDeaths ASC
    LIMIT 1;
  `;
  const stateWithLowestDeaths = await database.get(getStateWithLowestDeathsQuery);
  response.send(stateWithLowestDeaths);
});


// API 15: Get the percentage of active cases per state
app.get("/active-cases-percentage-by-state/", async (request, response) => {
  const getActiveCasesPercentageByStateQuery = `
    SELECT 
      state.state_name AS stateName,
      (SUM(district.active) * 100.0 / SUM(district.cases)) AS activeCasesPercentage
    FROM 
      state
    JOIN 
      district
    ON 
      state.state_id = district.state_id
    GROUP BY 
      state.state_name
    HAVING 
      SUM(district.cases) > 0;
  `;
  const activeCasesPercentageByState = await database.all(getActiveCasesPercentageByStateQuery);
  response.send(activeCasesPercentageByState);
});

// API 16: Get the average number of cases per district for each state
app.get("/average-cases-per-district-by-state/", async (request, response) => {
  const getAverageCasesPerDistrictByStateQuery = `
    SELECT 
      state.state_name AS stateName,
      AVG(district.cases) AS averageCasesPerDistrict
    FROM 
      state
    JOIN 
      district
    ON 
      state.state_id = district.state_id
    GROUP BY 
      state.state_name;
  `;
  const averageCasesPerDistrictByState = await database.all(getAverageCasesPerDistrictByStateQuery);
  response.send(averageCasesPerDistrictByState);
});

// API 17: Get the states where the number of active cases is more than 50% of total cases
app.get("/states-active-cases-more-than-50-percent/", async (request, response) => {
  const getStatesActiveCasesMoreThan50PercentQuery = `
    SELECT 
      state.state_name AS stateName,
      SUM(district.active) AS totalActiveCases,
      SUM(district.cases) AS totalCases,
      (SUM(district.active) * 100.0 / SUM(district.cases)) AS activeCasesPercentage
    FROM 
      state
    JOIN 
      district
    ON 
      state.state_id = district.state_id
    GROUP BY 
      state.state_name
    HAVING 
      activeCasesPercentage > 50;
  `;
  const statesActiveCasesMoreThan50Percent = await database.all(getStatesActiveCasesMoreThan50PercentQuery);
  response.send(statesActiveCasesMoreThan50Percent);
});

// API 18: Get the top 3 states with the highest cured to cases ratio
app.get("/top-3-states-highest-cured-to-cases-ratio/", async (request, response) => {
  const getTop3StatesHighestCuredToCasesRatioQuery = `
    SELECT 
      state.state_name AS stateName,
      SUM(district.cured) AS totalCured,
      SUM(district.cases) AS totalCases,
      (SUM(district.cured) * 100.0 / SUM(district.cases)) AS curedToCasesRatio
    FROM 
      state
    JOIN 
      district
    ON 
      state.state_id = district.state_id
    GROUP BY 
      state.state_name
    ORDER BY 
      curedToCasesRatio DESC
    LIMIT 3;
  `;
  const top3StatesHighestCuredToCasesRatio = await database.all(getTop3StatesHighestCuredToCasesRatioQuery);
  response.send(top3StatesHighestCuredToCasesRatio);
});

module.exports = app;
