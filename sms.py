#!/usr/bin/env python3
import csv
import logging
import os
import sys
import time
from dataclasses import dataclass

from yaroc.clients.mop import MOP
from yaroc.utils.modem_manager import ModemManager

logging.basicConfig(
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def parse_phone_numbers():
    assert len(sys.argv) >= 2
    phone_number_file = sys.argv[1]
    with open(phone_number_file, "r") as f:
        reader = csv.DictReader(f)
        parsed_info = [row for row in reader]
        by_name, by_card = {}, {}
        duplicated_names = set()
        for row in parsed_info:
            name = row["name"]
            phone_number = row["phone_number"]
            if phone_number == "":
                continue
            if row["card"]:
                try:
                    card = int(row["card"])
                    by_card[card] = phone_number
                except Exception:
                    logging.error("Failed to parse card for {name}")
            else:
                logging.info(f"Card empty for {name}")

            if name in by_name and name not in duplicated_names:
                logging.warning(f"Duplicate name {name}")
                duplicated_names.add(name)
                del by_name[name]
            elif name in duplicated_names:
                logging.warning(f"Duplicate name {name}")
            else:
                by_name[name] = phone_number
    return by_card, by_name


@dataclass
class SmsInfo:
    name: str
    card: int | None
    stat: int | None
    sms_text: str
    sms_id: int | None
    sms_status: str


def parse_sms_info(
    name: str, card: str, stat: str, sms_text: str, sms_id: str, sms_status: str
):
    parsed_card = int(card)
    parsed_stat = int(stat)
    parsed_sms_id = None if sms_id == "" else int(sms_id)
    return SmsInfo(
        card=parsed_card,
        name=name,
        stat=parsed_stat,
        sms_text=sms_text,
        sms_id=parsed_sms_id,
        sms_status=sms_status,
    )


def process_results():
    csv_file = "/home/lukas/sms.csv"
    modem_manager = ModemManager()
    modems = modem_manager.get_modems()
    modem = None if len(modems) == 0 else modems[0]
    if len(modems) == 0:
        logging.warning("Could not find any modem, continuing without sending SMS")

    if os.path.isfile(csv_file):
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            parsed_info = [parse_sms_info(**row) for row in reader]
            sms_infos = {info.card: info for info in parsed_info}
    else:
        sms_infos = {}
    write_header = not os.path.isfile(csv_file)

    f = open(csv_file, "a")
    fieldnames = ["card", "name", "stat", "sms_text", "sms_id", "sms_status"]
    csv_writer = csv.DictWriter(f, fieldnames=fieldnames)
    if write_header:
        csv_writer.writeheader()

    by_card, by_name = parse_phone_numbers()
    for result in MOP.results("192.168.1.10", 2009):
        card = result.competitor.card
        if card in sms_infos and sms_infos[card].sms_status == "sent":
            sms_info = sms_infos[card]
            logging.info(f"SMS to {card} already sent as SMS number {sms_info.sms_id}")
            continue

        name = result.competitor.name
        if card in by_card:
            number = by_card[card]
        elif name in by_name:
            number = by_name[name]
        else:
            logging.warning(f"Phone number of {name} unavailable")
            continue

        text = None
        match result.stat:
            case MOP.STAT_OK:
                text = (
                    f"Gratulujeme, {name}! Dobehli ste v čase {result.time}. "
                    "Online výsledky z Behu mesta Pezinok nájdete na https://live.sokolpezinok.sk"
                )
                logging.info(f"{name} dobehol/la v čase {result.time}")
            case MOP.STAT_MP:
                text = (
                    f"{name}, neprebehli ste celú trať. Kontaktujte rozhodcov Behu mesta Pezinok. "
                    "Online výsledky z Behu mesta Pezinok nájdete na https://live.sokolpezinok.sk"
                )
                logging.info(f"{name} chýba prebeh")
            case MOP.STAT_DNF:
                text = (
                    f"{name}, nezaznamenali sme prechod cieľom. Kontaktujte rozhodcov Behu mesta Pezinok. "
                    "Online výsledky z Behu mesta Pezinok nájdete na https://live.sokolpezinok.sk"
                )
                logging.info(f"{name} nedokončil(a)")
            case MOP.STAT_OOC:
                text = None
                logging.info(f"{name} bežal(a) mimo súťaže")
            case MOP.STAT_DNS:
                text = (
                    f"{name}, boužiaľ ste neštartovali na Behu mesta Pezinok. "
                    "Online výsledky z Behu mesta Pezinok nájdete na https://live.sokolpezinok.sk"
                )
                logging.info(f"{name} neštartoval")

        if text is not None and modem is not None:
            try:
                sms_path = modem_manager.create_sms(modem, number, text)
                sms_info = SmsInfo(
                    card=card,
                    name=name,
                    stat=result.stat,
                    sms_text=text,
                    sms_id=None,  # last part of the handle
                    sms_status="created",
                )
                logging.info(f"Sending SMS to {name}")
                modem_manager.send_sms(modem, sms_path)
                logging.info("Sent SMS")

                time.sleep(1.0)
                sms_info.sms_status = modem_manager.sms_status(sms_path)
                csv_writer.writerow(
                    {
                        "card": sms_info.card,
                        "name": sms_info.name,
                        "stat": sms_info.stat,
                        "sms_text": sms_info.sms_text,
                        "sms_id": sms_info.sms_id,
                        "sms_status": sms_info.sms_status,
                    }
                )
            except Exception as err:
                logging.error(err)


process_results()
