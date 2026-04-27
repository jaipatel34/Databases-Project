from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta

conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])

cur = conn.cursor()


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """
    raise NotImplementedError("you must implement this function")


def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    raise NotImplementedError("you must implement this function")


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    raise NotImplementedError("you must implement this function")


"""
item_id - A string containing the Item ID for the item being rented.
customer_id - A string containing the customer id of the customer renting the item.
"""
def rent_item(item_id: str = None, customer_id: str = None):
    todaysDate = date.today()

    # timedelta Reference https://docs.python.org/3/library/datetime.html
    dateDue = todaysDate + timedelta(days=14)

    # Insert Entry Into Rental
    cur.execute("INSERT INTO rental (item_id, customer_id, rental_date, due_date) "
                "VALUES (?, ?, ?, ?)", (item_id, customer_id, todaysDate, dateDue))


"""
Returns the customer's new place in line.
"""
def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    # Puts The Customer At Their Place In Line
    updatedSpot = line_length(item_id) + 1
    cur.execute("INSERT INTO waitlist (item_id, customer_id, place_in_line) "
                "VALUES (?, ?, ?)", (item_id, customer_id, updatedSpot))

    return updatedSpot


"""
Removes person at position 1 and shifts everyone else down by 1.
"""
def update_waitlist(item_id: str = None):
    # Delete Person 1
    cur.execute("DELETE FROM waitlist "
                "WHERE item_id = ? "
                "AND place_in_line = 1", (item_id,))

    # Shift Everyone Else Down 1
    cur.execute("UPDATE waitlist "
                "SET place_in_line = place_in_line - 1 "
                "WHERE item_id = ?", (item_id,))


"""
Moves a rental from rental to rental_history with return_date = today.
"""
def return_item(item_id: str = None, customer_id: str = None):
    # Gather
    cur.execute("SELECT rental_date, due_date "
                "FROM rental "
                "WHERE item_id = ? "
                "AND customer_id = ?", (item_id, customer_id))

    entry = cur.fetchone()

    # Check If Empty
    if entry is None:
        return
    # Insert Entry Into History
    cur.execute("INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date) "
                "VALUES (?, ?, ?, ?, ?)",
                (item_id, customer_id, entry[0], entry[1], date.today()))

    # Remove Entry From Rental
    cur.execute("DELETE FROM rental "
                "WHERE item_id = ? "
                "AND customer_id = ?",
                (item_id, customer_id))


"""
Adds 14 days to the due_date.
"""
def grant_extension(item_id: str = None, customer_id: str = None):
    # Add 14 Days To Date
    cur.execute("UPDATE rental "
                "SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY) "
                "WHERE item_id = ? AND customer_id = ?",
                (item_id, customer_id))


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


"""
Returns num_owned - active rentals. Returns -1 if item doesn't exist.
"""
def number_in_stock(item_id: str = None) -> int:
    # Get The Number Owned
    cur.execute("SELECT i_num_owned "
                "FROM item "
                "WHERE i_item_id = ?", (item_id,))
    entryOwned = cur.fetchone()

    if entryOwned is None:
        return -1

    # Get Active Rentals
    cur.execute("SELECT COUNT(*) "
                "FROM rental "
                "WHERE item_id = ?", (item_id,))
    entryRental = cur.fetchone()[0]

    return entryOwned[0] - entryRental


"""
Returns the customer's place_in_line, or -1 if not on waitlist.
"""
def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    # Get Customers Spot In Line
    cur.execute("SELECT place_in_line "
                "FROM waitlist "
                "WHERE item_id = ? "
                "AND customer_id = ?", (item_id, customer_id))
    entry = cur.fetchone()

    if entry is None:
        return -1

    return entry[0]


"""
Returns how many people are on the waitlist for this item.
"""
def line_length(item_id: str = None) -> int:
    # Get Number Of People On Waitlist
    cur.execute("SELECT COUNT(*) "
                "FROM waitlist "
                "WHERE item_id = ?", (item_id,))

    return cur.fetchone()[0]


"""
Commits all changes made to the db.
"""
def save_changes():
    # commit Reference https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection-commit.html
    conn.commit()


"""
Closes the cursor and connection.
"""
def close_connection():
    cur.close()
    conn.close()