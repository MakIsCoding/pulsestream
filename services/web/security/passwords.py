"""
Password hashing using bcrypt via passlib.

Bcrypt is the standard for password hashing — slow on purpose (defeats
brute force), salts automatically, no plaintext anywhere.

Usage:
    hashed = hash_password("plaintext")
    verify_password("plaintext", hashed)  # True/False
"""

from passlib.context import CryptContext


_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    """Returns a bcrypt hash of the plain password."""
    return _pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Returns True if the plain password matches the bcrypt hash."""
    return _pwd_context.verify(plain, hashed)