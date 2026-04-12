async def run_auction_timer(bot_instance: Bot, bot_id: str, auction_id: str):
    """
    Таймер аукциона.
    Отсчёт начинается ТОЛЬКО после первой ставки.
    Без ставок — аукцион просто ждёт.
    Отсчёт — в комментариях (reply на пересланный пост в группе).
    Победитель — постом в канале.
    """
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg:
        return

    try:
        # Ждём первой ставки — без ставок не начинаем отсчёт
        while True:
            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return

            # Если есть хотя бы одна ставка — начинаем логику отсчёта
            if auction.get('current_bidder') is not None:
                break

            # Ставок ещё нет — ждём 5 секунд и проверяем снова
            await asyncio.sleep(5)

        # Основной цикл отсчёта — запускается только когда есть ставки
        while True:
            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return

            channel_id = auction['channel']
            discussion_id = auction.get('discussion_chat_id')
            discussion_message_id = auction.get('discussion_message_id')
            message_id = auction['message_id']
            last_bid_time = datetime.fromisoformat(auction['last_bid_time'])
            snapshot_time = last_bid_time

            async def send_countdown(text: str):
                """Отправка цифры отсчёта как комментарий под постом."""
                # Приоритет: reply на пересланный пост в группе комментариев
                if discussion_id and discussion_message_id:
                    try:
                        await bot_instance.send_message(
                            chat_id=discussion_id,
                            text=text,
                            reply_to_message_id=discussion_message_id
                        )
                        return
                    except Exception as e:
                        logger.error(f"Ошибка reply в группу: {e}")

                # Fallback: просто в группу без reply
                if discussion_id:
                    try:
                        await bot_instance.send_message(
                            chat_id=discussion_id,
                            text=text
                        )
                        return
                    except Exception as e:
                        logger.error(f"Ошибка отправки в группу: {e}")

                # Fallback: в канал с reply на пост
                try:
                    await bot_instance.send_message(
                        chat_id=channel_id,
                        text=text,
                        reply_to_message_id=message_id
                    )
                except Exception as e:
                    logger.error(f"Ошибка отсчёта {text}: {e}")

            # Ждём 2 минуты с последней ставки
            wait_2min = last_bid_time + timedelta(minutes=2)
            now = datetime.now()
            if now < wait_2min:
                await asyncio.sleep((wait_2min - now).total_seconds())

            # Перечитываем — могла прийти новая ставка
            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            # Обновляем discussion_message_id — мог прийти пока ждали
            discussion_message_id = auction.get('discussion_message_id')
            current_last = datetime.fromisoformat(auction['last_bid_time'])
            if current_last > snapshot_time:
                logger.info(f"Аукцион {auction_id}: новая ставка во время ожидания, сброс")
                continue

            # Пишем "3" в комментариях
            await send_countdown("3")
            logger.info(f"Аукцион {auction_id}: отсчёт 3")
            await asyncio.sleep(30)

            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                logger.info(f"Аукцион {auction_id}: новая ставка после '3', сброс")
                continue

            # Пишем "2" в комментариях
            await send_countdown("2")
            logger.info(f"Аукцион {auction_id}: отсчёт 2")
            await asyncio.sleep(30)

            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                logger.info(f"Аукцион {auction_id}: новая ставка после '2', сброс")
                continue

            # Пишем "1" в комментариях
            await send_countdown("1")
            logger.info(f"Аукцион {auction_id}: отсчёт 1")
            await asyncio.sleep(30)

            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                logger.info(f"Аукцион {auction_id}: новая ставка после '1', сброс")
                continue

            # === ОБЪЯВЛЕНИЕ ПОБЕДИТЕЛЯ ПОСТОМ В КАНАЛЕ ===
            winner_id = auction.get('current_bidder')
            winner_amount = auction.get('current_bid', 0)
            auction['finished'] = True
            config.save()

            if winner_id:
                winner_display = get_user_display_name(winner_id)
                winner_balance = db.get_balance(winner_id, bot_id)

                if winner_balance != float('inf') and winner_balance < winner_amount:
                    try:
                        target = discussion_id if discussion_id else channel_id
                        await bot_instance.send_message(
                            chat_id=target,
                            text=(
                                f"⚠️ У {winner_display} недостаточно средств "
                                f"({winner_amount} {bot_cfg.currency_emoji}). "
                                f"Аукцион отменён."
                            )
                        )
                    except Exception as e:
                        logger.error(f"Ошибка сообщения о нехватке средств: {e}")
                else:
                    db.deduct_balance(winner_id, bot_id, winner_amount)

                    try:
                        await bot_instance.send_message(
                            chat_id=bot_cfg.takes_channel,
                            text=f"Победитель: {winner_display}"
                        )
                        logger.info(
                            f"Аукцион {auction_id}: победитель {winner_display}, "
                            f"списано {winner_amount} {bot_cfg.currency_emoji}"
                        )
                    except Exception as e:
                        logger.error(f"Ошибка объявления победителя: {e}")

                    try:
                        await bot_instance.send_message(
                            winner_id,
                            f"🏆 Вы выиграли аукцион!\n"
                            f"💰 Списано: {winner_amount} {bot_cfg.currency_emoji}"
                        )
                    except Exception:
                        pass

            if auction_id in config.active_auctions:
                del config.active_auctions[auction_id]
                config.save()
            return

    except asyncio.CancelledError:
        logger.info(f"Аукцион {auction_id} отменён")
    except Exception as e:
        logger.error(f"Ошибка аукциона: {e}")
