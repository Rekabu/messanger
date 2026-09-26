from flask import Flask, render_template, request, send_from_directory, session, redirect, url_for, flash, jsonify
from flask_socketio import SocketIO, emit, join_room
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from functools import wraps
from datetime import datetime
import sqlite3
import os
import eventlet
eventlet.monkey_patch(os=False, thread=False)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get("SECRET_KEY", "секретный-ключ-поменяй-меня")

UPLOAD_FOLDER = "uploads"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# --- Ограничения на загрузку файлов ---
app.config["MAX_CONTENT_LENGTH"] = 15 * 1024 * 1024  # 15 МБ максимум на файл
ALLOWED_EXTENSIONS = {
    "png", "jpg", "jpeg", "gif", "webp",       # изображения
    "pdf", "txt", "doc", "docx", "xls", "xlsx", # документы
    "zip", "rar", "7z",                        # архивы
    "mp3", "wav", "mp4", "mov"                  # медиа
}

socketio = SocketIO(app, async_mode='eventlet')

PUBLIC_ROOM = "Всем"
MESSAGES_PAGE_SIZE = 30

# username -> множество socket.io session id (для поддержки нескольких вкладок/устройств)
online_users = {}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def is_group_recipient(recipient):
    """Публичный чат и групповые комнаты - настоящие socket.io комнаты,
    куда уже вступили все участники, поэтому им достаточно одного emit()."""
    return recipient == PUBLIC_ROOM or recipient.startswith("room:")


# ---------- База данных ----------

def get_db():
    conn = sqlite3.connect("chat.db")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            recipient TEXT,
            text TEXT,
            time TEXT,
            file_path TEXT,
            read INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rooms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_by TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS room_members (
            room_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            PRIMARY KEY (room_id, username)
        )
    """)
    # миграция для уже существующих баз, где колонки read ещё нет
    try:
        cursor.execute("ALTER TABLE messages ADD COLUMN read INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # колонка уже есть
    conn.commit()
    conn.close()


def create_user(username, password):
    conn = get_db()
    cursor = conn.cursor()
    password_hash = generate_password_hash(password)
    cursor.execute(
        "INSERT INTO users (username, password_hash) VALUES (?, ?)",
        (username, password_hash)
    )
    conn.commit()
    conn.close()


def get_user(username):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    conn.close()
    return row


def user_exists(username):
    return get_user(username) is not None


def save_message(username, recipient, text, time, file_path=None):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO messages (username, recipient, text, time, file_path) VALUES (?, ?, ?, ?, ?)",
        (username, recipient, text, time, file_path)
    )
    conn.commit()
    message_id = cursor.lastrowid
    conn.close()
    return message_id


def mark_messages_read(peer, me):
    """Отмечает прочитанными все сообщения, которые peer прислал мне (me) лично."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE messages SET read = 1 WHERE username = ? AND recipient = ? AND read = 0",
        (peer, me)
    )
    conn.commit()
    conn.close()


def get_message(message_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
    row = cursor.fetchone()
    conn.close()
    return row


def update_message_text(message_id, text):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE messages SET text = ? WHERE id = ?", (text, message_id))
    conn.commit()
    conn.close()


def delete_message_by_id(message_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM messages WHERE id = ?", (message_id,))
    conn.commit()
    conn.close()


def row_to_message_dict(row):
    return {
        "id": row["id"],
        "username": row["username"],
        "recipient": row["recipient"],
        "text": row["text"],
        "time": row["time"],
        "file_path": ("/uploads/" + row["file_path"]) if row["file_path"] else None,
        "read": row["read"],
    }


def fetch_messages_page(recipient, me, before_id=None):
    """Возвращает (messages, has_more) - до MESSAGES_PAGE_SIZE сообщений диалога/комнаты,
    отсортированных по времени (старые -> новые), плюс более старая страница подгружается через before_id."""
    conn = get_db()
    cursor = conn.cursor()

    if is_group_recipient(recipient):
        where = "recipient = ?"
        params = [recipient]
    else:
        # личный диалог: сообщения, где я <-> peer в любую сторону
        where = "((username = ? AND recipient = ?) OR (username = ? AND recipient = ?))"
        params = [me, recipient, recipient, me]

    if before_id:
        where += " AND id < ?"
        params.append(before_id)

    query = f"""
        SELECT id, username, recipient, text, time, file_path, read FROM messages
        WHERE {where}
        ORDER BY id DESC
        LIMIT ?
    """
    params.append(MESSAGES_PAGE_SIZE + 1)  # +1 чтобы узнать, есть ли ещё более старые

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    has_more = len(rows) > MESSAGES_PAGE_SIZE
    rows = rows[:MESSAGES_PAGE_SIZE]
    rows.reverse()  # обратно в хронологический порядок

    return [row_to_message_dict(r) for r in rows], has_more


# ---------- Группы ----------

def create_room(name, creator, member_usernames):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO rooms (name, created_by) VALUES (?, ?)", (name, creator))
    room_id = cursor.lastrowid

    all_members = set(member_usernames)
    all_members.add(creator)
    for m in all_members:
        cursor.execute(
            "INSERT OR IGNORE INTO room_members (room_id, username) VALUES (?, ?)",
            (room_id, m)
        )
    conn.commit()
    conn.close()
    return room_id


def get_user_rooms(username):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT rooms.id, rooms.name FROM rooms
        JOIN room_members ON rooms.id = room_members.room_id
        WHERE room_members.username = ?
        ORDER BY rooms.id
    """, (username,))
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r["id"], "name": r["name"]} for r in rows]


def is_room_member(room_id, username):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM room_members WHERE room_id = ? AND username = ?",
        (room_id, username)
    )
    row = cursor.fetchone()
    conn.close()
    return row is not None


def room_id_from_recipient(recipient):
    """'room:12' -> 12, либо None если формат не подходит."""
    if not recipient.startswith("room:"):
        return None
    try:
        return int(recipient.split(":", 1)[1])
    except (ValueError, IndexError):
        return None


# ---------- Авторизация ----------

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "username" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not username or not password:
            flash("Заполните имя пользователя и пароль")
            return redirect(url_for("register"))

        if len(password) < 6:
            flash("Пароль должен быть не короче 6 символов")
            return redirect(url_for("register"))

        if get_user(username):
            flash("Такой пользователь уже существует")
            return redirect(url_for("register"))

        create_user(username, password)
        session["username"] = username
        return redirect(url_for("index"))

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        user = get_user(username)
        if user is None or not check_password_hash(user["password_hash"], password):
            flash("Неверное имя пользователя или пароль")
            return redirect(url_for("login"))

        session["username"] = username
        return redirect(url_for("index"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.pop("username", None)
    return redirect(url_for("login"))


# ---------- Основные маршруты ----------

@app.route("/")
@login_required
def index():
    username = session["username"]
    rooms = get_user_rooms(username)
    return render_template("index.html", username=username, public_room=PUBLIC_ROOM, rooms=rooms)


@app.route("/rooms/create", methods=["POST"])
@login_required
def create_room_route():
    username = session["username"]
    name = request.form.get("name", "").strip()
    members_raw = request.form.get("members", "")
    member_names = [m.strip() for m in members_raw.split(",") if m.strip()]

    if not name:
        flash("Укажите название группы")
        return redirect(url_for("index"))

    unknown = [m for m in member_names if not user_exists(m)]
    if unknown:
        flash("Пользователи не найдены: " + ", ".join(unknown))
        return redirect(url_for("index"))

    create_room(name, username, member_names)
    return redirect(url_for("index"))


@app.route("/api/messages")
@login_required
def api_messages():
    me = session["username"]
    recipient = request.args.get("with", "").strip()
    before_id = request.args.get("before_id", type=int)

    if not recipient:
        return jsonify({"error": "missing 'with' parameter"}), 400

    room_id = room_id_from_recipient(recipient)
    if room_id is not None and not is_room_member(room_id, me):
        return jsonify({"error": "not a member of this room"}), 403

    messages, has_more = fetch_messages_page(recipient, me, before_id)
    return jsonify({"messages": messages, "has_more": has_more})


@app.route("/upload", methods=["POST"])
@login_required
def upload():
    file = request.files.get("file")
    username = session["username"]  # берём из сессии, а не из формы
    recipient = request.form.get("recipient", PUBLIC_ROOM)

    room_id = room_id_from_recipient(recipient)
    if room_id is not None and not is_room_member(room_id, username):
        return jsonify({"error": "Вы не состоите в этой группе"}), 403

    if not file or not file.filename:
        return jsonify({"error": "Файл не выбран"}), 400

    if not allowed_file(file.filename):
        allowed = ", ".join(sorted(ALLOWED_EXTENSIONS))
        return jsonify({"error": f"Недопустимый тип файла. Разрешены: {allowed}"}), 400

    filename = secure_filename(file.filename)
    # избегаем перезаписи одноимённых файлов от разных пользователей
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{filename}"
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(filepath)

    time = datetime.now().strftime("%H:%M")
    message_id = save_message(username, recipient, "", time, filename)

    payload = {
        "id": message_id,
        "username": username,
        "recipient": recipient,
        "text": "",
        "time": time,
        "file_path": "/uploads/" + filename,
        "read": 0
    }

    socketio.emit("message", payload, to=recipient)
    if not is_group_recipient(recipient):
        socketio.emit("message", payload, to=username)

    return jsonify({"ok": True}), 200


@app.errorhandler(413)
def file_too_large(e):
    return jsonify({"error": "Файл слишком большой (максимум 15 МБ)"}), 413


@app.route("/uploads/<path:filename>")
@login_required
def uploaded_file(filename):
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)


# ---------- Socket.IO ----------

@socketio.on('connect')
def handle_connect():
    if "username" not in session:
        return False  # отклоняем подключение неавторизованных

    username = session["username"]
    join_room(username)
    join_room(PUBLIC_ROOM)

    # вступаем во все свои групповые комнаты
    for room in get_user_rooms(username):
        join_room(f"room:{room['id']}")

    is_first_connection = username not in online_users
    online_users.setdefault(username, set()).add(request.sid)

    if is_first_connection:
        emit("user_online", {"username": username}, to=PUBLIC_ROOM, include_self=False)

    emit("online_users", {"users": list(online_users.keys())})


@socketio.on('disconnect')
def handle_disconnect():
    username = session.get("username")
    if not username or username not in online_users:
        return

    online_users[username].discard(request.sid)
    if not online_users[username]:
        del online_users[username]
        emit("user_offline", {"username": username}, to=PUBLIC_ROOM, include_self=False)


@socketio.on('typing')
def handle_typing(data):
    if "username" not in session:
        return

    username = session["username"]
    recipient = data.get("recipient", PUBLIC_ROOM)
    payload = {"username": username, "recipient": recipient}
    emit("typing", payload, to=recipient, include_self=False)


@socketio.on('mark_read')
def handle_mark_read(data):
    if "username" not in session:
        return

    me = session["username"]
    peer = data.get("peer")

    if not peer or is_group_recipient(peer):
        return  # статус прочтения только для личных 1:1 диалогов

    mark_messages_read(peer, me)
    emit("messages_read", {"reader": me}, to=peer)


@socketio.on('message')
def handle_message(data):
    if "username" not in session:
        return  # игнорируем сообщения от неавторизованных

    username = session["username"]  # доверяем только серверной сессии
    recipient = data.get("recipient", PUBLIC_ROOM)
    text = (data.get("text") or "").strip()

    if not text:
        return

    room_id = room_id_from_recipient(recipient)
    if room_id is not None and not is_room_member(room_id, username):
        return  # не даём слать сообщения в чужие группы

    time = datetime.now().strftime("%H:%M")
    message_id = save_message(username, recipient, text, time, None)

    payload = {
        "id": message_id,
        "username": username,
        "recipient": recipient,
        "text": text,
        "time": time,
        "file_path": None,
        "read": 0
    }

    emit("message", payload, to=recipient)
    if not is_group_recipient(recipient):
        emit("message", payload, to=username)


@socketio.on('edit_message')
def handle_edit_message(data):
    if "username" not in session:
        return

    username = session["username"]
    message_id = data.get("id")
    new_text = (data.get("text") or "").strip()

    if not message_id or not new_text:
        return

    row = get_message(message_id)
    if row is None or row["username"] != username:
        return  # нельзя редактировать чужое сообщение

    update_message_text(message_id, new_text)

    payload = {"id": message_id, "text": new_text}
    recipient = row["recipient"]

    emit("message_edited", payload, to=recipient)
    if not is_group_recipient(recipient):
        emit("message_edited", payload, to=username)


@socketio.on('delete_message')
def handle_delete_message(data):
    if "username" not in session:
        return

    username = session["username"]
    message_id = data.get("id")

    if not message_id:
        return

    row = get_message(message_id)
    if row is None or row["username"] != username:
        return  # нельзя удалить чужое сообщение

    recipient = row["recipient"]
    file_path = row["file_path"]
    delete_message_by_id(message_id)

    if file_path:
        full_path = os.path.join(app.config["UPLOAD_FOLDER"], os.path.basename(file_path))
        try:
            if os.path.exists(full_path):
                os.remove(full_path)
        except OSError:
            pass  # файл уже мог быть удалён вручную - не критично

    payload = {"id": message_id}
    emit("message_deleted", payload, to=recipient)
    if not is_group_recipient(recipient):
        emit("message_deleted", payload, to=username)


if __name__ == "__main__":
    init_db()
    socketio.run(app, debug=True)