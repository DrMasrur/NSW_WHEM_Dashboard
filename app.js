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

function toPercentage(occupiedBeds, totalBeds) {
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
  avgOccupancy.textContent = `${Math.round(toPercentage(bedTotals.occupied, bedTotals.total))}%`;
  availableBeds.textContent = String(Math.max(bedTotals.total - bedTotals.occupied, 0));
}

function renderBars(rows) {
  occupancyChart.replaceChildren();

  rows.forEach((row) => {
    const pct = Math.round(toPercentage(row.occupiedBeds, row.totalBeds));

    const wrapper = document.createElement('div');
    wrapper.className = 'bar-row';
    const label = document.createElement('span');
    label.className = 'bar-label';
    label.textContent = row.facility;

    const track = document.createElement('div');
    track.className = 'bar-track';

    const fill = document.createElement('div');
    fill.className = 'bar-fill';
    fill.style.width = `${pct}%`;
    track.appendChild(fill);

    const value = document.createElement('span');
    value.className = 'bar-value';
    value.textContent = `${pct}%`;

    wrapper.appendChild(label);
    wrapper.appendChild(track);
    wrapper.appendChild(value);

    occupancyChart.appendChild(wrapper);
  });
}

function renderTable(rows) {
  facilityTableBody.replaceChildren();

  rows.forEach((row) => {
    const pct = Math.round(toPercentage(row.occupiedBeds, row.totalBeds));
    const tr = document.createElement('tr');
    const values = [row.facility, row.district, row.totalBeds, row.occupiedBeds, `${pct}%`];
    values.forEach((value) => {
      const td = document.createElement('td');
      td.textContent = String(value);
      tr.appendChild(td);
    });

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
  districtFilter.replaceChildren();
  districts.forEach((district) => {
    const option = document.createElement('option');
    option.value = district;
    option.textContent = district;
    districtFilter.appendChild(option);
  });
  districtFilter.addEventListener('change', renderDashboard);
}

initializeFilter();
renderDashboard();
