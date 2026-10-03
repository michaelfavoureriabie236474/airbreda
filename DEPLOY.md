# Deploying AirBreda (my runbook)

These are the steps I used to deploy AirBreda on AWS eu-north-1 (Stockholm). Values in `<angle brackets>` are placeholders. **[laptop]** means PowerShell on my laptop; **[VM]** means after logging in to the VM over SSH.

## 1. AWS resources (console)

- **EC2:** one t3.micro with Amazon Linux 2023, an 8 GB gp3 disk and a key pair. Security group: SSH (22) from My IP, port 8000 from anywhere. Outbound: all traffic to 0.0.0.0/0.
- **RDS:** PostgreSQL, Free tier template, Single-AZ, db.t4g.micro, initial database name `airbreda`, "Connect to an EC2 compute resource" so the database stays private.
- **S3:** a private bucket for the traffic files.
- **IAM:** a role for EC2 with an inline policy: `s3:PutObject` and `s3:GetObject` on `arn:aws:s3:::<bucket>/*`, and `s3:ListBucket` on `arn:aws:s3:::<bucket>`. Attach the role to the VM.

## 2. Connect to the VM [laptop]

```powershell
ssh -o ServerAliveInterval=30 -i $HOME\.ssh\airbreda-key.pem ec2-user@<VM-public-IP>
```

If it times out, my IP has changed: EC2 → Security Groups → inbound rules → SSH source → My IP. Change only the **inbound** rule; the outbound rule must stay "All traffic to 0.0.0.0/0", or the VM can't reach the APIs, S3 or the database.

## 3. Prepare the VM [VM]

```bash
sudo dnf update -y
sudo dnf install -y docker git postgresql15 cronie
sudo systemctl enable --now docker crond
sudo usermod -aG docker ec2-user
exit
```

Log in again so the docker group applies, then check with `docker run --rm hello-world`.

## 4. Code, secrets and tables [VM]

```bash
git clone https://github.com/<github-username>/airbreda.git
cd airbreda
cp .env.example .env
nano .env          # DB_HOST, DB_PASSWORD, S3_BUCKET, AWS_REGION=eu-north-1, DB_SSLMODE=require
chmod 600 .env
set -a; source .env; set +a
PGPASSWORD="$DB_PASSWORD" psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER sslmode=require" -f schema.sql
```

No AWS keys go in `.env`: the VM uses its IAM role.

## 5. Collectors and schedule [VM]

```bash
docker build -t airbreda-air .
docker build -t airbreda-traffic -f Dockerfile.traffic .
docker run --rm --env-file .env airbreda-air
docker run --rm --env-file .env airbreda-traffic
```

Both should end with `run_complete`. Then schedule them for minute 0 of every hour:

```bash
mkdir -p ~/airbreda/logs
(crontab -l 2>/dev/null
 echo "0 * * * * /usr/bin/docker run --rm --env-file /home/ec2-user/airbreda/.env airbreda-air >> /home/ec2-user/airbreda/logs/air.log 2>&1"
 echo "0 * * * * /usr/bin/docker run --rm --env-file /home/ec2-user/airbreda/.env airbreda-traffic >> /home/ec2-user/airbreda/logs/traffic.log 2>&1"
) | crontab -
```

cron needs the full path `/usr/bin/docker`. Logs go to `logs/*.log` because `--rm` deletes each container after its run.

## 6. Train and start the dashboard [VM]

Run these one at a time:

```bash
docker build -t airbreda-trainer -f Dockerfile.train .
mkdir -p out
docker run --rm --env-file .env -v /home/ec2-user/airbreda/out:/app/out airbreda-trainer
cat out/model_report.json
cp out/model.pkl .
docker build -t airbreda-dashboard -f Dockerfile.dashboard .
docker run -d --name dashboard --restart unless-stopped --env-file .env -p 8000:8000 airbreda-dashboard
```

Then open `http://<VM-public-IP>:8000`. Some campus networks block port 8000; mobile data works.

To retrain later: run the trainer again, `cp out/model.pkl .`, `docker rm -f dashboard`, rebuild the image and start it again.

## 7. Checks

- `crontab -l` shows the two schedule lines.
- `tail -n 2 ~/airbreda/logs/air.log ~/airbreda/logs/traffic.log` shows `run_complete` within the last hour.
- `http://<VM-public-IP>:8000/health` shows recent `last_successful_fetch` times for both sources.

## 8. Shutting down

Terminate the EC2 instance, delete the RDS database, empty and delete the bucket, and remove the unused security groups.
