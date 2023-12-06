const sleep = (milliseconds) => { return new Promise(resolve => setTimeout(resolve, milliseconds)) }

function parse_date(date_string) {
    let year = date_string.substring(0, 4);
    let month = date_string.substring(4, 6);
    let day = date_string.substring(6, 8);
    let hour = date_string.substring(9, 11);
    let minute = date_string.substring(11, 13);
    let second = date_string.substring(13, 15);
    // JavaScript counts months from 0 to 11. January is 0, and December is 11.
    let date = new Date(year, month - 1, day, hour, minute, second);
    return date;
}

async function get_active_terminals() {
    while (true) {
        /* Get terminal statuses */
        let response = await fetch("/api/info/terminals/active");
        let data = await response.json();
        /* Update terminal statuses */
        let table_body = document.getElementById("dashboard-terminals-table-body");
        table_body.innerHTML = "";
        for (let i = 0; i < data.data.length; i++) {
            let row = document.createElement("tr");
            let terminal_id = document.createElement("td");
            let last_communication = document.createElement("td");
            // Pad RECEIVED_TIME on left with 0's to make it 6 characters long
            let last_communication_time = data.data[i].RECEIVED_TIME;
            last_communication_time = last_communication_time.padStart(6, '0');
            let timestamp = parse_date(data.data[i].RECEIVED_DATE + " " + last_communication_time);
            terminal_id.innerHTML = data.data[i].TERMINAL_ID;
            last_communication.innerHTML = timestamp.toLocaleString();
            row.appendChild(terminal_id);
            row.appendChild(last_communication);
            table_body.appendChild(row);
        }
        // Update number of active terminals
        let active_number = document.getElementById("dashboard-terminals-count");
        active_number.innerHTML = data.count;
        /* Sleep for 60 seconds */
        await sleep(60000);
    
    }
}

async function get_last_10_alarms() {
    while (true) {
        /* Get last 10 alarms */
        let response = await fetch("/api/info/terminals/alarms?limit=10");
        let data = await response.json();
        /* Update last 10 alarms */
        let table_body = document.getElementById("dashboard-alarms-table-body");
        table_body.innerHTML = "";
        for (let i = 0; i < data.data.length; i++) {
            let row = document.createElement("tr");
            let terminal_id = document.createElement("td");
            let alarm_type = document.createElement("td");
            let alarm_time = document.createElement("td");
            // Pad RECEIVED_TIME on left with 0's to make it 6 characters long
            let alarm_time_string = data.data[i].RECEIVED_TIME;
            alarm_time_string = alarm_time_string.padStart(6, '0');
            let timestamp = parse_date(data.data[i].RECEIVED_DATE + " " + alarm_time_string);
            terminal_id.innerHTML = data.data[i].TERMINAL_ID;
            alarm_type.innerHTML = data.data[i].ALARM;
            alarm_time.innerHTML = timestamp.toLocaleString();
            row.appendChild(terminal_id);
            row.appendChild(alarm_time);
            row.appendChild(alarm_type);
            table_body.appendChild(row);
        }
        /* Sleep for 60 seconds */
        await sleep(60000);
    }
}

window.onload = async () => {
    /* Get active terminals once every 60 seconds */
    get_active_terminals();
    get_last_10_alarms();
}