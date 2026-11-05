# High-Throughput Idempotent Transaction Ingestion Pipeline

*(Az angol nyelvű leíráshoz görgess lejjebb. / For the English version, please scroll down.)*

---

## MAGYAR NYELVŰ ÖSSZEFOGLALÓ

### Mi a projekt lényege?
A projekt egy nagy áteresztőképőségű, elosztott tranzakció-feldolgozó adatcsatorna (Data Pipeline) prototípusa, amelyet valós idejű, kritikus pénzügyi rendszerek mintájára terveztem. A fő cél az volt, hogy a beérkező, nagy mennyiségű tranzakciós adatot aszinkron módon, a HTTP API szálait nem blokkolva dolgozzuk fel, miközben garantáljuk az adatbázis-szintű konzisztenciát, a duplikációk szűrését (idempotencia) és a hibás adatok automata izolációját (DLQ).

Mivel korábban más technológiákkal dolgoztam, ez a projekt egyben az új .NET 10-es és C# 14-es képességek éles próbája is volt számomra, ahol a modern nyelvi elemeket valós, ipari problémák megoldására használtam.

---

### Rendszerfelépítés és architektúra
A rendszer három fő, egymástól teljesen függetlenül skálázható komponensből áll (Decoupled Architecture), amelyek Docker környezetben futnak:
1. **Adatgenerátor és Szimulátor (Python):** Végtelenített ciklusban futó script, amely valósághű tranzakciókat generál (UUID, JSON formátum), és hálózati szinten, valutatípus alapján (`tx.currency.huf` és `tx.currency.fx`) irányítja az adatokat a bróker felé.
2. **Üzenetbróker (RabbitMQ):** A rendszer idegrendszere. Egy `Topic Exchange`-et használ az üzenetek intelligens szétosztására, így a hazai (HUF) és a nemzetközi (FX) tranzakciók már hálózati szinten elkülönülnek. Itt konfiguráltam be a Dead Letter Exchange (DLX) mechanizmust is.
3. **Ingestion Engine & API Core (.NET 10):** A backend két párhuzamosan futó, nem-blokkoló `BackgroundService`-t futtat az üzenetek fogyasztására. Az írási ág egy szálbiztos memóriapufferen keresztül dolgozik, míg az olvasási ág (Kestrel Web API) teljesen különálló végpontokon szolgálja ki a statisztikai lekérdezéseket.

---

### Alkalmazott technológiák (The Stack)
- **Backend:** .NET 10 (C# 14), Entity Framework Core (PostgreSQL Provider)
- **Üzenetküldés:** RabbitMQ (Topic Exchange, DLX, Custom Routing Keys)
- **Adatbázis:** PostgreSQL 15 (Docker)
- **Szimulációs kliens:** Python 3 (Pika AMQP driver)
- **Infrastruktúra:** Docker, Docker Compose, SmartGit a verziókövetéshez

---

### 💻 Telepítési és Beállítási Útmutató (Local Setup Guide)

Ahhoz, hogy a teljes backend és szimulációs infrastruktúra egy másik fejlesztői gépen is zökkenőmentesen elinduljon, és pontosan ugyanaz az adatbázis-szerkezet épüljön fel, kövesd az alábbi lépéseket:

#### 1. Előfeltételek (Prerequisites)
Győződj meg róla, hogy a gépedre fel vannak telepítve a következők:
- **Docker Desktop** (A PostgreSQL és RabbitMQ futtatásához)
- **.NET 10 SDK** (A C# backend fordításához és futtatásához)
- **Python 3.10+** (A szimulátor futtatásához)

#### 2. Az Infrastruktúra elindítása (Docker)
Navigálj a projekt gyökérmappájába (ahol a `docker-compose.yml` található), és indítsd el a háttérkörnyezetet:
```bash
docker compose up -d
```
*Ez letölti és elindítja a PostgreSQL-t a `5432`-es porton, és a RabbitMQ Management felületet a `15672`-es porton (alapértelmezett belépés: guest/guest).*

#### 3. Az Adatbázis séma felépítése (.NET Migráció)
Az adatbázis sémájának, tábláinak és az egyedi indexeknek az automatikus felépítéséért az Entity Framework Core felel. Nem kell manuálisan SQL scripteket futtatni, a .NET a meglévő migrációs állományokból magától felépíti a struktúrát a Docker konténeren belül.
Nyiss egy terminált a `Backend/` mappában, és futtasd az alábbi parancsot:
```bash
# Globális EF Core eszközök telepítése (ha még nincs fent a gépeden)
dotnet tool install --global dotnet-ef

# Adatbázis sémájának frissítése és táblák automatikus létrehozása
dotnet ef database update
```
*Ez a parancs beolvassa a kód struktúráját, rácsatlakozik a háttérben futó Docker Postgres konténerre, és másodpercek alatt létrehozza a `transactions` és `faulty_transactions` táblákat a Unique Index szabályokkal együtt.*

#### 4. A Python Szimulátor felkészítése
Lépj be a `Simulator/` mappába. Hozz létre egy elkülönített virtuális környezetet, hogy a szükséges AMQP driverek ne ütközzenek a globális rendszereddel:
```bash
# Virtuális környezet létrehozása
python -m venv .venv

# Aktiválás (Windows)
.venv\Scripts\activate

# Szükséges függőségek telepítése
pip install pika requests
```

#### 5. A Pipeline indítása és ellenőrzése
1. **Backend indítása:** A `Backend/` mappában futtasd a `dotnet run` parancsot. A C# háttérszolgáltatások automatikusan felépítik a csatornákat a RabbitMQ-val, és az API elkezd hallgatni a `http://localhost:5095` címen.
2. **Szimulátor indítása:** A `Simulator/` mappában (aktivált .venv mellett) futtasd a `python main.py` parancsot. A Python letölti a törzsadatokat és ontani kezdi az üzeneteket.
3. **Ellenőrzés:** Nyiss meg egy böngészőt a `http://localhost:5095/Transaction/statistics` címen, hogy ellenőrizd az élőben gyűlő adatbázis-statisztikákat.

---

### Valódi architektúrális kihívások és mérnöki döntések

A fejlesztés során nem triviális, mély technológiai problémákat kellett megoldani, amelyek közvetlenül érintik a rendszer stabilitását:

#### 1. A szálbiztossági dilemma és a kritikus szakasz blokkolása
A legkomolyabb fejtörést a memóriában történő pufferelés (Batching) és a háttérszálak szinkronizációja okozta. A RabbitMQ fogyasztója aszinkron módon, párhuzamosan ontja az üzeneteket, miközben azokat egy megosztott belső listába gyűjtjük. Ha két üzenet egyszerre érkezik meg, és mindkettő módosítani akarja a listát, az `Array/List` belső indexelése összeomlik (`Concurrency Exception`).
- **Mérnöki megoldás:** Egy robusztus `lock` (kritikus szakasz) mechanizmust vezettem be a puffer módosítása köré. Amikor a lista eléri az 50-es méretet, a háttérszál zárolja a gyűjtőt, készít egy gyors másolatot (klónt) a memóriában lévő adatokról, kiüríti az eredeti puffert, majd azonnal feloldja a `lock`-ot. Így a RabbitMQ fogyasztója szinte azonnal mehet tovább dolgozni, miközben a nehéz, lassú adatbázis-mentés (`SaveChangesAsync`) már a zároláson kívül, a háttérben fut le a klónozott adathalmazzal.

#### 2. Az elszivárgó idők és az Idempotencia határai
Pénzügyi rendszereknél a kettős könyvelés (duplikáció) megengedhetetlen, de mi történik, ha egy hálózati hiba miatt a RabbitMQ kétszer küldi el ugyanazt az üzenetet?
- **Mérnöki döntés:** Kétlépcsős védelmi vonalat építettem ki. Első körben a kód szinten egy gyors, aszinkron `AnyAsync(t => t.TransactionId == id)` szűréssel ellenőrizzük, hogy létezik-e már a rekord. Mivel azonban a mentés csomagokban, kötegelve történik, előfordulhat olyan versenyhelyzet (Race Condition), hogy egy duplikált üzenet még azelőtt beérkezik, hogy az első csomagot az EF Core fizikailag kiírta volna az SQL-be. Ezt a kód szintű szűrő még nem látja.
- **A végső védvonal:** A problémát adatbázis-szinten zártam le. A PostgreSQL-ben a `transaction_id` oszlopra egy egyedi indexet (`Unique Index`) kényszerítettem ki a Fluent API-val. Ha a versenyhelyzet miatt be is csúszik egy duplikáció a kötegbe, a PostgreSQL tranzakció-kezelése atomi szinten észleli az ütközést, elutasítja a hibás beszúrást, így garantálva a golyóálló adatkonzisztenciát.

#### 3. A pipeline eldugulásának megakadályozása (DLQ mechanizmus)
Ha egy tranzakció hibás (pl. nem támogatott `XYZ` valutanemmel érkezik), és az EF Core megpróbálná betolni a validációs hiba ellenére, a PostgreSQL egy durva hibával az egész 50-es csomagot elutasítaná és visszagörgetné. Ezzel 49 jó tranzakció is elveszne egyetlen rossz miatt.
- **Mérnöki megoldás:** A validációt teljesen előrehoztam a csatorna elejére (`FluentValidation`). Ha az üzenet elbukik, a fő fogyasztó negatív nyugtát (`BasicNack(requeue: false)`) küld vissza a RabbitMQ-nak. A bróker a beépített Dead Letter Exchange (DLX) segítségével ezt azonnal átdobja a `dead_letter` sorra. Egy teljesen különálló háttérszolgáltatás, az `InvalidTransactionConsumerService` kizárólag ezt a hiba-sort figyeli, és a hibás tételeket aszinkron módon kiírja egy külön `FaultyTransactions` audit táblába. Így a fő adatcső egyetlen másodpercre sem lassul le, and a hibás adatok is transzparensek maradnak.

---
---

## ENGLISH VERSION (README)

### Project Overview
This project is a prototype of a high-throughput, distributed transaction ingestion pipeline modeled after real-time, critical financial systems. The primary goal is to ingest, validate, and batch process large volumes of transaction data asynchronously without blocking HTTP API threads, while guaranteeing database-level consistency, duplication filtering (idempotency), and automated isolation of malformed data (DLQ).

Having previously worked with other enterprise stacks, this project also served as a hands-on exploration of the latest **.NET 10** and **C# 14** features, applying modern language constructs to solve real-world architectural problems.

---

### Architecture & System Design
The system consists of three decoupled, independently scalable components running in a Dockerized environment:
1. **Transaction Simulator (Python):** A script running in an infinite loop that generates realistic financial transactions (UUIDs, structured JSON format) and routes them via AMQP based on currency types (`tx.currency.huf` and `tx.currency.fx`).
2. **Message Broker (RabbitMQ):** The nervous system of the application. It utilizes a `Topic Exchange` for intelligent message routing, separating domestic (HUF) and international (FX) traffic at the network level. It also handles the Dead Letter Exchange (DLX) configuration.
