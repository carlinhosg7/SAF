from flask import Blueprint
from utils.decorators import login_required, role_required

admin_bp = Blueprint("admin", __name__)

@admin_bp.route("/admin")
@login_required
@role_required("admin", "diretor")
def admin():
    return "<h1>Área Administrativa</h1>"