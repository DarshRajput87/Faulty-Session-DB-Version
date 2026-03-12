// =====================================================
// 📦 IMPORTS
// =====================================================
import axios from "axios";
import fs from "fs";
import path from "path";
import XLSX from "xlsx";
import nodemailer from "nodemailer";
import { MongoClient, ObjectId } from "mongodb";
import { PARTY_CONFIG } from "./config/partyConfig.js";

// =====================================================
// ⚙ CONFIG
// =====================================================
const TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2NDJlZTBkNmU1MmIzYjg1MWNmN2MxMjkiLCJhdXRoVG9rZW5WZXJzaW9uIjoidjEiLCJpYXQiOjE3NzMwNjA0MTgsImV4cCI6MTc3NDM1NjQxOCwidHlwZSI6ImFjY2VzcyJ9.X7lkB-6dkRAKw7gio-EOfn1nUi55B1cXkCsbS2s5Am0";

const API_URL =
    "https://appapi.chargecloud.net/v1/report/emspFaultyBookings";

const PROD_URI =
    "mongodb+srv://IT_INTERN:ITINTERN123@cluster1.0pycd.mongodb.net";

const AUTO_URI =
    "mongodb+srv://DarshRajputApp:tst4I6oi6m77xXJS@cluster0.jfptrcd.mongodb.net";

const todayFolder =
    new Date().toISOString().split("T")[0];

const reportDir =
    path.join("reports", todayFolder);

if (!fs.existsSync(reportDir))
    fs.mkdirSync(reportDir, { recursive: true });

const excelPath =
    path.join(reportDir, "faulty.xlsx");

const lockFile = "process.lock";

// =====================================================
// LOGGER
// =====================================================
function log(step, msg) {
    console.log(`[${new Date().toISOString()}] [${step}] ${msg}`);
}

// =====================================================
// LOCK
// =====================================================
function acquireLock() {

    if (fs.existsSync(lockFile)) {
        log("LOCK", "Another instance running");
        process.exit(0);
    }

    fs.writeFileSync(lockFile, process.pid.toString());
}

function releaseLock() {
    if (fs.existsSync(lockFile))
        fs.unlinkSync(lockFile);
}

// =====================================================
// RETRY
// =====================================================
async function retry(fn, retries = 3, delay = 3000) {

    try {
        return await fn();
    }
    catch (err) {

        if (retries <= 0) throw err;

        log("RETRY", `Retrying... (${retries})`);

        await new Promise(r =>
            setTimeout(r, delay)
        );

        return retry(fn, retries - 1, delay);
    }
}

// =====================================================
// MAIL
// =====================================================
const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
        user: "darshraj3104@gmail.com",
        pass: "ddxg ddtb fiiz mygh"
    }
});

// =====================================================
// MAIL BODY
// =====================================================
function buildMailText({ type, partyId, batch, count }) {

    if (type === "Notification") {
        return `Hello,

We have not received the Charge Detail Record (CDR) for the session(s) listed in the attached file. These sessions appear to be in a faulty state and the corresponding CDRs have not yet been received from your end.

Kindly review the sessions, close them if required, and push the corresponding CDRs from your system.

Session Details:
Please refer to the attached Excel file for the list of affected sessions.

This will help ensure that the sessions are accurately reflected in billing and reporting.

Regards,
Chargezone`;
    }

    if (type === "Reminder1") {
        return `Hello,

This is a reminder regarding the faulty session(s) for which the Charge Detail Record (CDR) is still pending.

Our records indicate that the CDRs for the session(s) listed in the attached file have not yet been received.

Kindly review the sessions, close them if required, and push the corresponding CDRs from your system at the earliest.

Session Details:
Please refer to the attached Excel file for the list of affected sessions.

Submitting the CDRs will help ensure that the sessions are accurately reflected in billing and reporting.

Regards,
Chargezone`;
    }

    if (type === "FinalReminder") {
        return `Hello,

This is a final reminder regarding the faulty session(s) for which the Charge Detail Record (CDR) is still pending.

Despite previous notifications, the CDRs for the session(s) listed in the attached file have not yet been received.

We request you to kindly review the sessions, close them if required, and push the corresponding CDRs from your system as soon as possible.

Session Details:
Please refer to the attached Excel file for the list of affected sessions.

Prompt action will help ensure that the sessions are accurately reflected in billing and reporting.

Regards,
Chargezone`;
    }

}

// =====================================================
// ATTACHMENT
// =====================================================
function createMailBuffer(rows) {

    const removed = new Set([
        "Vehicle ID",
        "Vehicle Make",
        "Vehicle Model",
        "VIN Number",
        "User Name",
        "User Phone"
    ]);

    const cleaned = rows.map(r => {

        const o = {};

        Object.keys(r).forEach(k => {
            if (!removed.has(k.trim()))
                o[k] = r[k];
        });

        return o;
    });

    const ws = XLSX.utils.json_to_sheet(cleaned);

    const wb = XLSX.utils.book_new();

    XLSX.utils.book_append_sheet(
        wb,
        ws,
        "Faulty"
    );

    return XLSX.write(wb, {
        type: "buffer",
        bookType: "xlsx"
    });
}

// =====================================================
// DB
// =====================================================
const prodClient =
    new MongoClient(PROD_URI);

const autoClient =
    new MongoClient(AUTO_URI);

let bookingCollection;
let faultyCollection;

async function connectDB() {

    await prodClient.connect();
    await autoClient.connect();

    bookingCollection =
        prodClient
            .db("chargezoneprod")
            .collection("chargerbookings");

    faultyCollection =
        autoClient
            .db("ChargeZoneOperationEngine")
            .collection("ocpi_emsp_faulty_session");

    log("DB", "Connected Prod + Automation");
}

// =====================================================
// FETCH BOOKINGS
// =====================================================
async function fetchBookingsBulk(ids) {

    const validIds =
        ids
            .filter(id => ObjectId.isValid(id))
            .map(id => new ObjectId(id));

    const docs =
        await bookingCollection
            .find({
                _id: { $in: validIds }
            })
            .toArray();

    const map = new Map();

    docs.forEach(d =>
        map.set(String(d._id), d)
    );

    return map;
}

// =====================================================
// DOWNLOAD EXCEL
// =====================================================
async function downloadExcel() {

    const now = new Date();

    const istParts =
        new Intl.DateTimeFormat(
            "en-GB",
            {
                timeZone: "Asia/Kolkata",
                year: "numeric",
                month: "2-digit",
                day: "2-digit",
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
                hour12: false
            }
        )
            .formatToParts(now)
            .reduce((acc, p) => {
                if (p.type !== "literal")
                    acc[p.type] = Number(p.value);
                return acc;
            }, {});

    const fromISO =
        new Date(Date.UTC(
            istParts.year,
            istParts.month - 1,
            1,
            -5,
            -30,
            0
        )).toISOString();

    const toISO =
        new Date(Date.UTC(
            istParts.year,
            istParts.month - 1,
            istParts.day,
            istParts.hour - 5,
            istParts.minute - 30,
            istParts.second
        )).toISOString();

    log("API",
        `Downloading Range → ${fromISO} → ${toISO}`
    );

    const response =
        await retry(() =>
            axios.post(
                API_URL,
                {
                    payment_status: "action_required",
                    excel: true,
                    from: fromISO,
                    to: toISO
                },
                {
                    responseType: "arraybuffer",
                    headers: {
                        authorization: `Bearer ${TOKEN}`,
                        "content-type": "application/json"
                    }
                }
            )
        );

    fs.writeFileSync(
        excelPath,
        response.data
    );

    log("API",
        "Excel Downloaded");
}

// =====================================================
// FAULT CHECK
// =====================================================
function isFaulty(doc, partyId) {

    const party =
        PARTY_CONFIG[partyId];

    if (!party) return false;

    const credential =
        doc.ocpiCredential
            ? String(doc.ocpiCredential)
            : null;

    if (!party.ocpiCredentials.includes(credential))
        return false;

    return (
        doc.is_ocpi_based_booking &&
        doc.is_emsp_based_booking &&
        !doc.invoice &&
        Array.isArray(doc.faulty_booking_reason) &&
        doc.faulty_booking_reason.length > 0 &&
        doc.payment_status === "action_required"
    );
}

// =====================================================
// CORE LOGIC
// =====================================================
function normalizeToMinute(date) {
    const d = new Date(date);
    d.setSeconds(0);
    d.setMilliseconds(0);
    return d;
}
async function reconcileAndProcess() {

    const REMINDER_DELAY = 24 * 60 * 60 * 1000;
    const FINAL_DELAY = 24 * 60 * 60 * 1000;
    const now = normalizeToMinute(normalizeToMinute(new Date()));

    const workbook = XLSX.readFile(excelPath);

    const jsonData = XLSX.utils.sheet_to_json(
        workbook.Sheets[workbook.SheetNames[0]],
        { range: 2 }
    );

    const partyMap = {};

    jsonData.forEach(r => {

        const partyId = String(r["Party ID"]).trim();

        if (!partyId) return;

        if (!partyMap[partyId])
            partyMap[partyId] = [];

        partyMap[partyId].push(r);

    });

    // =====================================================
    // PARTY LOOP
    // =====================================================

    for (const [partyId, rows] of Object.entries(partyMap)) {

        log("PROCESS", partyId);

        const bookingIds =
            rows.map(r => r["Authorization Reference"]);

        const bookingMap =
            await fetchBookingsBulk(bookingIds);

        const dbFaultyRows =
            rows.filter(r => {

                const doc =
                    bookingMap.get(
                        String(r["Authorization Reference"])
                    );

                return doc && isFaulty(doc, partyId);

            });

        const todayIds =
            dbFaultyRows
                .filter(r =>
                    ObjectId.isValid(r["Authorization Reference"])
                )
                .map(r =>
                    new ObjectId(r["Authorization Reference"])
                );

        // =============================================
        // STILL EXIST LOGIC
        // =============================================

        await faultyCollection.updateMany(
            { partyId },
            {
                $set: {
                    still_exist: false,
                    still_exist_timestamp: normalizeToMinute(normalizeToMinute(new Date()))
                }
            }
        );

        await faultyCollection.updateMany(
            {
                partyId,
                bookingId: { $in: todayIds }
            },
            {
                $set: {
                    still_exist: true,
                    still_exist_timestamp: normalizeToMinute(normalizeToMinute(new Date()))
                }
            }
        );

        const existingDocs =
            await faultyCollection.find({
                partyId,
                bookingId: { $in: todayIds }
            }).toArray();

        const existingMap = new Map();

        existingDocs.forEach(d =>
            existingMap.set(String(d.bookingId), d)
        );

        // =============================================
        // DETECT NEW BOOKINGS
        // =============================================

        const newRows =
            dbFaultyRows.filter(r => {

                const id =
                    r["Authorization Reference"];

                if (!ObjectId.isValid(id))
                    return false;

                return !existingMap.has(String(id));

            });

        // =============================================
        // SEND NOTIFICATION
        // =============================================

        if (newRows.length) {

            const buffer =
                createMailBuffer(newRows);

            const info =
                await transporter.sendMail({

                    to: PARTY_CONFIG[partyId].emails.join(","),
                    cc: PARTY_CONFIG[partyId].cc?.join(","),

                    subject:
                        `[AUTO-Notification] Faulty Sessions - ${partyId} - ${todayFolder}`,

                    text:
                        buildMailText({
                            type: "Notification",
                            partyId,
                            batch: todayFolder,
                            count: newRows.length
                        }),

                    attachments: [{
                        filename: `${partyId}_Faulty.xlsx`,
                        content: buffer
                    }]
                });

            const docs = newRows.map(r => {

                const booking =
                    bookingMap.get(
                        String(r["Authorization Reference"])
                    );

                return {

                    bookingId:
                        new ObjectId(r["Authorization Reference"]),

                    // ✅ from chargerbookings collection
                    tenant_id:
                        booking?.tenant
                            ? new ObjectId(booking.tenant)
                            : null,

                    // ✅ charger id from booking document
                    charger_station_id:
                        booking?.charger
                            ? new ObjectId(booking.charger)
                            : null,

                    // ✅ vehicle id from booking document
                    vehicle_id:
                        booking?.vehicle
                            ? new ObjectId(booking.vehicle)
                            : null,

                    partyId,

                    station_name: r["Station Name"],
                    city: r["City"],
                    state: r["State"],

                    connector_id: Number(r["Connector ID"]),
                    energy_consumed: Number(r["Energy Consumed"]),

                    faulty_reasons:
                        r["Faulty Reasons"]
                            ? [r["Faulty Reasons"]]
                            : [],

                    invoice_id: booking?.invoice || null,

                    mail_history: [{
                        type: "Notification",
                        timestamp: normalizeToMinute(new Date()),
                        thread_id: info.messageId
                    }],

                    still_exist: true,
                    still_exist_timestamp: normalizeToMinute(new Date()),
                    created_at: normalizeToMinute(new Date())
                };

            });
            const ops =
                docs.map(doc => ({
                    updateOne: {
                        filter: { bookingId: doc.bookingId },
                        update: { $setOnInsert: doc },
                        upsert: true
                    }
                }));

            await faultyCollection.bulkWrite(ops);

            log("MAIL", `Notification ${partyId}`);
        }

        // =============================================
        // GROUPED REMINDER PROCESSING
        // =============================================

        const reminder1Rows = [];
        const finalReminderRows = [];
        const reminder1Docs = [];
        const finalReminderDocs = [];

        for (const row of dbFaultyRows) {

            const id =
                String(row["Authorization Reference"]);

            const dbDoc =
                existingMap.get(id);

            if (!dbDoc) continue;
            if (!dbDoc.still_exist) continue;

            const lastMail =
                dbDoc.mail_history[
                dbDoc.mail_history.length - 1
                ];

            const diff =
                now - new Date(lastMail.timestamp);

            if (
                lastMail.type === "Notification" &&
                diff > REMINDER_DELAY
            ) {

                reminder1Rows.push(row);
                reminder1Docs.push(dbDoc);

            }

            else if (
                lastMail.type === "Reminder1" &&
                diff > FINAL_DELAY
            ) {

                finalReminderRows.push(row);
                finalReminderDocs.push(dbDoc);

            }

        }

        // SEND REMINDER 1
        if (reminder1Rows.length) {

            const buffer =
                createMailBuffer(reminder1Rows);

            const info =
                await transporter.sendMail({

                    to: PARTY_CONFIG[partyId].emails.join(","),
                    cc: PARTY_CONFIG[partyId].cc?.join(","),

                    subject:
                        `[AUTO-Reminder1] Faulty Sessions - ${partyId} - ${todayFolder}`,

                    text:
                        buildMailText({
                            type: "Reminder1",
                            partyId,
                            batch: todayFolder,
                            count: reminder1Rows.length
                        }),

                    attachments: [{
                        filename: `${partyId}_Faulty.xlsx`,
                        content: buffer
                    }]
                });

            await faultyCollection.updateMany(
                { _id: { $in: reminder1Docs.map(d => d._id) } },
                {
                    $push: {
                        mail_history: {
                            type: "Reminder1",
                            timestamp: normalizeToMinute(new Date()),
                            thread_id: info.messageId
                        }
                    }
                }
            );

            log("MAIL", `Reminder1 ${partyId}`);
        }

        // SEND FINAL REMINDER
        if (finalReminderRows.length) {

            const buffer =
                createMailBuffer(finalReminderRows);

            const info =
                await transporter.sendMail({

                    to: PARTY_CONFIG[partyId].emails.join(","),
                    cc: PARTY_CONFIG[partyId].cc?.join(","),

                    subject:
                        `[AUTO-FinalReminder] Faulty Sessions - ${partyId} - ${todayFolder}`,

                    text:
                        buildMailText({
                            type: "FinalReminder",
                            partyId,
                            batch: todayFolder,
                            count: finalReminderRows.length
                        }),

                    attachments: [{
                        filename: `${partyId}_Faulty.xlsx`,
                        content: buffer
                    }]
                });

            await faultyCollection.updateMany(
                { _id: { $in: finalReminderDocs.map(d => d._id) } },
                {
                    $push: {
                        mail_history: {
                            type: "FinalReminder",
                            timestamp: normalizeToMinute(new Date()),
                            thread_id: info.messageId
                        }
                    }
                }
            );

            log("MAIL", `FinalReminder ${partyId}`);
        }

    }

}

// =====================================================
// RUN
// =====================================================
async function run() {

    acquireLock();

    log("SYSTEM", "Started");

    try {

        await connectDB();

        await downloadExcel();

        await reconcileAndProcess();

        log("SYSTEM", "Completed");

    }
    catch (e) {

        log("ERROR", e.message);

    }
    finally {

        releaseLock();

        await prodClient.close();
        await autoClient.close();

        log("SYSTEM", "Stopped");

        process.exit(0);
    }
}

run();
