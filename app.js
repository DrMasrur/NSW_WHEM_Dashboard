const facilityData = [
  { facility: 'Westmead Hospital', district: 'Western Sydney', totalBeds: 120, occupiedBeds: 94 },
  { facility: 'Liverpool Hospital', district: 'South Western Sydney', totalBeds: 140, occupiedBeds: 121 },
  { facility: 'Royal Prince Alfred', district: 'Sydney', totalBeds: 160, occupiedBeds: 138 },
  { facility: 'John Hunter Hospital', district: 'Hunter New England', totalBeds: 110, occupiedBeds: 83 },
  { facility: 'Orange Health Service', district: 'Western NSW', totalBeds: 80, occupiedBeds: 56 },
  { facility: 'St George Hospital', district: 'South Eastern Sydney', totalBeds: 100, occupiedBeds: 79 }
];

const districtFilter = document.getElementById('districtFilter');
const totalFacilities = document.getElementById('totalFacilities');
const avgOccupancy = document.getElementById('avgOccupancy');
const availableBeds = document.getElementById('availableBeds');
const occupancyChart = document.getElementById('occupancyChart');
const facilityTableBody = document.getElementById('facilityTableBody');

function toPct(occupiedBeds, totalBeds) {
  return totalBeds ? (occupiedBeds / totalBeds) * 100 : 0;
}

function getFilteredFacilities() {
  const selected = districtFilter.value;
  if (!selected || selected === 'All') {
    return facilityData;
  }

  return facilityData.filter((item) => item.district === selected);
}

function renderSummary(rows) {
  const facilities = rows.length;
  const bedTotals = rows.reduce(
    (acc, item) => {
      acc.total += item.totalBeds;
      acc.occupied += item.occupiedBeds;
      return acc;
    },
    { total: 0, occupied: 0 }
  );

  totalFacilities.textContent = String(facilities);
  avgOccupancy.textContent = `${Math.round(toPct(bedTotals.occupied, bedTotals.total))}%`;
  availableBeds.textContent = String(Math.max(bedTotals.total - bedTotals.occupied, 0));
}

function renderBars(rows) {
  occupancyChart.innerHTML = '';

  rows.forEach((row) => {
    const pct = Math.round(toPct(row.occupiedBeds, row.totalBeds));

    const wrapper = document.createElement('div');
    wrapper.className = 'bar-row';
    wrapper.innerHTML = `
      <span class="bar-label">${row.facility}</span>
      <div class="bar-track"><div class="bar-fill" style="width: ${pct}%"></div></div>
      <span class="bar-value">${pct}%</span>
    `;

    occupancyChart.appendChild(wrapper);
  });
}

function renderTable(rows) {
  facilityTableBody.innerHTML = '';

  rows.forEach((row) => {
    const pct = Math.round(toPct(row.occupiedBeds, row.totalBeds));
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.facility}</td>
      <td>${row.district}</td>
      <td>${row.totalBeds}</td>
      <td>${row.occupiedBeds}</td>
      <td>${pct}%</td>
    `;

    facilityTableBody.appendChild(tr);
  });
}

function renderDashboard() {
  const rows = getFilteredFacilities();
  renderSummary(rows);
  renderBars(rows);
  renderTable(rows);
}

function initializeFilter() {
  const districts = ['All', ...new Set(facilityData.map((item) => item.district))];
  districtFilter.innerHTML = districts.map((district) => `<option>${district}</option>`).join('');
  districtFilter.addEventListener('change', renderDashboard);
}

initializeFilter();
renderDashboard();
