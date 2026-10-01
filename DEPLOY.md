# Deploying AirBreda to the VM (runbook)

Region: **eu-north-1**. Replace everything in `<angle brackets>`.
Commands marked **[laptop]** run in PowerShell on your laptop; **[VM]** run after you've SSH'd in.

---

## Part 1: get the code onto GitHub [laptop] (10 min)

1. On github.com create a new **public** repository called `airbreda` (no README, no .gitignore).
2. Unzip `airbreda.zip`, then in PowerShell:

```powershell
cd <path>\airbreda
git init
git add .
git status            # CHECK: no .env and no .pem file in the list
git commit -m "AirBreda ingestion and dashboard code"
git branch -M main
git remote add origin https://github.com/<your-github-username>/airbreda.git
git push -u origin main
```

Why public: the grader needs to read the repo, and GitHub Pages is free for public repos.
That's also why `.env` (passwords) is in `.gitignore`.

---

## Part 2: connect to the VM [laptop] (5 min)

```powershell
ssh -i $HOME\.ssh\airbreda-key.pem ec2-user@<VM-public-IP>
```

Type `yes` the first time. If you get **"UNPROTECTED PRIVATE KEY FILE"**, Windows thinks other
users can read your key. Fix it, then retry:

```powershell
icacls $HOME\.ssh\airbreda-key.pem /inheritance:r
icacls $HOME\.ssh\airbreda-key.pem /grant:r "$($env:USERNAME):(R)"
```

If it just hangs: your IP changed since you created the security group. EC2 → Security groups →
airbreda-vm-sg → Edit inbound rules → SSH source → My IP → Save.

---

## Part 3: prepare the VM [VM] (10 min)

```bash
sudo dnf update -y
sudo dnf install -y docker git postgresql15 cronie
sudo systemctl enable --now docker crond
sudo usermod -aG docker ec2-user
exit
```

Reconnect with the same `ssh` command (the docker group only applies after logging in again), then:

```bash
docker run --rm hello-world        # expect: "Hello from Docker!"
```

What these are: `docker` runs containers, `git` downloads your code, `postgresql15` gives us
`psql` to create the tables, `cronie` is the hourly scheduler (Amazon Linux 2023 doesn't include
it by default). `enable --now` = start it now AND after every reboot.

---

## Part 4: code + secrets + tables [VM] (10 min)

```bash
git clone https://github.com/<your-github-username>/airbreda.git
cd airbreda
cp .env.example .env
nano .env
```

In nano, set: `DB_HOST=<RDS endpoint>`, `DB_PASSWORD=<your password>`, `S3_BUCKET=<bucket>`,
keep `DB_USER=airbreda_admin`, `DB_NAME=airbreda`, `DB_SSLMODE=require`, `AWS_REGION=eu-north-1`.
Save: **Ctrl+O, Enter, Ctrl+X**. Then:

```bash
chmod 600 .env                       # only you can read the passwords file
set -a; source .env; set +a          # load the values into this shell
PGPASSWORD="$DB_PASSWORD" psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER sslmode=require" -f schema.sql
```

Expected: `CREATE TABLE`, `CREATE TABLE`, `CREATE INDEX`.
- `database "airbreda" does not exist` → you skipped "Initial database name". Run the same psql
  command with `dbname=postgres` and `-c "CREATE DATABASE airbreda;"`, then rerun the schema.
- It hangs → the database security group doesn't allow the VM. Check RDS → airbreda-db →
  Connectivity & security → the VM's security group must be allowed on port 5432.

Note: no AWS keys go in `.env`. The VM uses its IAM role (`airbreda-ec2-role`) automatically.

---

## Part 5: build, test once, schedule [VM] (15 min)

```bash
docker build -t airbreda-air .
docker build -t airbreda-traffic -f Dockerfile.traffic .

docker run --rm --env-file .env airbreda-air        # last line: "run_complete" ... "inserted": ~50
docker run --rm --env-file .env airbreda-traffic    # last line: "run_complete" ... "sites_saved": 4

PGPASSWORD="$DB_PASSWORD" psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER sslmode=require" \
  -c "SELECT count(*), max(timestamp) FROM sensor_readings;"
aws s3 ls s3://$S3_BUCKET/ndw/ --recursive           # expect 4 .csv files
```

Each container's own Day 2 health check (reads the last runs from the database):

```bash
docker run --rm --env-file .env airbreda-air python ingest_air.py health
docker run --rm --env-file .env airbreda-traffic python ingest_traffic.py health
```

Only when both work, schedule them every hour:

```bash
mkdir -p ~/airbreda/logs
(crontab -l 2>/dev/null
 echo "0 * * * * /usr/bin/docker run --rm --env-file /home/ec2-user/airbreda/.env airbreda-air >> /home/ec2-user/airbreda/logs/air.log 2>&1"
 echo "0 * * * * /usr/bin/docker run --rm --env-file /home/ec2-user/airbreda/.env airbreda-traffic >> /home/ec2-user/airbreda/logs/traffic.log 2>&1"
) | crontab -
crontab -l        # shows the two lines
```

`0 * * * *` = minute 0 of every hour. The full path `/usr/bin/docker` is needed because cron
runs with an almost empty PATH (Day 3 warning). Logs are appended to `logs/*.log` because
`--rm` deletes the container (and its logs) after each run.

**After the next full hour**, check: `tail -n 3 ~/airbreda/logs/*.log`

---

## Part 6 (Thursday ~17:00): train + dashboard [VM]

```bash
cd ~/airbreda && git pull
docker build -t airbreda-trainer -f Dockerfile.train .
mkdir -p out
docker run --rm --env-file .env -v /home/ec2-user/airbreda/out:/app/out airbreda-trainer
cat out/model_report.json            # rows, R2, MAE, coefficients -> these go in ADR-006

cp out/model.pkl out/training_data.csv .
docker build -t airbreda-dashboard -f Dockerfile.dashboard .
docker run -d --name dashboard --restart unless-stopped --env-file .env -p 8000:8000 airbreda-dashboard
curl -s localhost:8000/health
```

Then open `http://<VM-public-IP>:8000` on your laptop **and on your phone using mobile data**
(proves it's reachable from outside your network, like the grader).

`--restart unless-stopped`: Docker restarts the dashboard after a crash or a VM reboot.
The ingestion jobs survive a reboot because `crond` was enabled in Part 3.

**Retrain Friday morning** (more data): run the trainer again, copy the model, then
`docker rm -f dashboard`, rebuild the image, and run it again.

---

## Part 7 (Day 4): tighten the VM's permissions

Replace `AmazonS3FullAccess` on `airbreda-ec2-role` with this inline policy (IAM → Roles →
airbreda-ec2-role → Add permissions → Create inline policy → JSON):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"],
     "Resource": "arn:aws:s3:::<bucket>/*"},
    {"Effect": "Allow", "Action": "s3:ListBucket",
     "Resource": "arn:aws:s3:::<bucket>"}
  ]
}
```

Then detach `AmazonS3FullAccess` and run `docker run --rm --env-file .env airbreda-traffic` once
to confirm it still works. Note `s3:ListBucket` is needed on top of the course's Put/Get, because
the trainer and the dashboard *list* the files to find them.

---

## After the course (within 48 hours)

Terminate the EC2 instance, delete the RDS database (no final snapshot), empty then delete the bucket.
