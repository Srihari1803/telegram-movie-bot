def start_health_server() -> None:
    try:
        port = int(os.getenv("PORT", "8080"))
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        logger.error("PORT must be an integer between 1 and 65535")
        return

    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)

    logger.info("Health server listening on port %d", port)

    server.serve_forever()



# ════════════════════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    logger.info("✅ Bot starting up...")
    logger.info("BOT_TOKEN present:    %s", bool(BOT_TOKEN))
    logger.info("TMDB_API_KEY present: %s", bool(TMDB_API_KEY))
    logger.info("MISTRAL_API_KEY present: %s", bool(MISTRAL_API_KEY))
    logger.info("SUPABASE configured:   %s", supabase is not None)
    logger.info("Working directory:    %s", os.getcwd())
    logger.info("Bot script location:  %s", _BASE_DIR)
    logger.info("DB file:              %s", DB_FILE)
    logger.info("sites.json path:      %s", SITES_FILE)
    logger.info("sites.json exists:    %s", os.path.isfile(SITES_FILE))

    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing from environment / .env")
    init_db()
    try:
        removed = cleanup_cache()
        if removed:
            logger.info("Startup cache cleanup: removed %d stale entries", removed)
    except Exception:
        logger.exception("Startup cache cleanup failed; continuing")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",     start))
    app.add_handler(CommandHandler("help",      help_command))
    app.add_handler(CommandHandler("ai",        ai_command))
    app.add_handler(CommandHandler("sites",     sites_command))
    app.add_handler(CommandHandler("movie",     movie_command))
    app.add_handler(CommandHandler("tv",        tv_command))
    app.add_handler(CommandHandler("anime",     anime_command))
    app.add_handler(CommandHandler("game",      game_command))
    app.add_handler(CommandHandler("favorites", favorites_command))
    app.add_handler(CommandHandler("history",   history_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_title))
    app.add_handler(CallbackQueryHandler(favorite_callback))

    if app.job_queue is not None:
        app.job_queue.run_repeating(
            cleanup_cache_job,
            interval=CACHE_CLEANUP_INTERVAL,
            first=CACHE_CLEANUP_INTERVAL,
        )
        logger.info(
            "JobQueue cache-cleanup scheduled every %ds", CACHE_CLEANUP_INTERVAL
        )
    else:
        logger.warning(
            "JobQueue not available (install python-telegram-bot[job-queue] for "
            "scheduled cleanup). Falling back to daemon thread."
        )
        start_fallback_cleanup_thread()

    # Set Telegram command menu
    async def post_init(application):
        try:
            await application.bot.set_my_commands([
                BotCommand("start", "Start the bot"),
                BotCommand("help", "Show help"),
                BotCommand("ai", "Ask the AI"),
                BotCommand("movie", "Search for a movie"),
                BotCommand("tv", "Search for a TV show"),
                BotCommand("anime", "Search for anime"),
                BotCommand("game", "Search for a game"),
                BotCommand("sites", "Show configured sites"),
                BotCommand("favorites", "Show favorites"),
                BotCommand("history", "Show search history"),
            ])
        except Exception:
            logger.exception("Could not register Telegram command menu")

    app.post_init = post_init
    
    health_thread = threading.Thread(
        target=start_health_server,
        daemon=True,
        name="health-server",
    )
    health_thread.start()

    logger.info("Bot is polling...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("❌ Startup error:")
        traceback.print_exc()
        raise
