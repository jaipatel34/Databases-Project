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
    #Fetchone reference: https://www.geeksforgeeks.org/dbms/querying-data-from-a-database-using-fetchone-and-fetchall/

    cur.execute("SELECT MAX(i_item_sk) FROM item")
    row = cur.fetchone()
    if row[0] is None:
        my_sk = 1
    else:
        my_sk = row[0] + 1
    rec_start_date = f"{new_item.start_year}-01-01"

    # You are inserting a new item into the item table

    cur.execute("""
        INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name,
                          i_brand, i_class, i_category, i_manufact,
                          i_current_price, i_num_owned)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
    (my_sk, new_item.item_id, rec_start_date, new_item.product_name,
          new_item.brand, None, new_item.category, new_item.manufact,
          new_item.current_price, new_item.num_owned))
    

def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    street_number = new_customer.address.split(",")[0].strip().split(" ", 1)[0]
    street_name = new_customer.address.split(",")[0].strip().split(" ", 1)[1]
    city_name = new_customer.address.split(",")[1].strip()
    state = new_customer.address.split(",")[2].strip().split(" ")[0]
    zip_code = new_customer.address.split(",")[2].strip().split(" ")[1]
    first_name = new_customer.name.split(" ", 1)[0]
    last_name = new_customer.name.split(" ", 1)[1]

    # You are inserting a new customer and their address into the database

    cur.execute("SELECT MAX(ca_address_sk) FROM customer_address")
    row = cur.fetchone()
    if row[0] is None:
        new_addr_sk = 1
    else:
        new_addr_sk = row[0] + 1

    cur.execute("""
        INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name,
                                      ca_city, ca_state, ca_zip)
        VALUES (?, ?, ?, ?, ?, ?)
                """, 
    (new_addr_sk, street_number, street_name, city_name, state, zip_code))

    cur.execute("SELECT MAX(c_customer_sk) FROM customer")
    row = cur.fetchone()
    if row[0] is None:
        my_cust_sk = 1
    else:
        my_cust_sk = row[0] + 1

    cur.execute("""
        INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, c_last_name,
                              c_email_address, c_current_addr_sk)
        VALUES (?, ?, ?, ?, ?, ?)
                """, 
    (my_cust_sk, new_customer.customer_id, first_name, last_name,
          new_customer.email, new_addr_sk))


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    # You are updatingan existing customer's information in the database
    if new_customer.name is not None:
        first_name = new_customer.name.split(" ", 1)[0]
        last_name = new_customer.name.split(" ", 1)[1]
        cur.execute("""
            UPDATE customer 
            SET c_first_name = ?, c_last_name = ?
            WHERE c_customer_id = ?
                    """, 
        (first_name, last_name, original_customer_id))

    if new_customer.email is not None:
        cur.execute("""
            UPDATE customer 
            SET c_email_address = ?
            WHERE c_customer_id = ?
                    """, 
        (new_customer.email, original_customer_id))

    if new_customer.address is not None:
        street_number = new_customer.address.split(",")[0].strip().split(" ", 1)[0]
        street_name = new_customer.address.split(",")[0].strip().split(" ", 1)[1]
        city_name = new_customer.address.split(",")[1].strip()
        state = new_customer.address.split(",")[2].strip().split(" ")[0]
        zip_code = new_customer.address.split(",")[2].strip().split(" ")[1]
        cur.execute("""
            UPDATE customer_address
            SET ca_street_number = ?, ca_street_name = ?, ca_city = ?, ca_state = ?, ca_zip = ?
            WHERE ca_address_sk = (
                SELECT c_current_addr_sk FROM customer WHERE c_customer_id = ?
            )
                    """, 
        (street_number, street_name, city_name, state, zip_code, original_customer_id))

    if new_customer.customer_id is not None:
        cur.execute("""
            UPDATE customer 
            SET c_customer_id = ?
            WHERE c_customer_id = ?
                    """, 
        (new_customer.customer_id, original_customer_id))


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
    # You are returning a list of items matching the given filters
    query = """
        SELECT i_item_id, i_product_name, i_brand, i_category,
               i_manufact, i_current_price, YEAR(i_rec_start_date), i_num_owned
        FROM item WHERE 1=1
    """
    res = []
    if use_patterns == True:
        oper = "LIKE"
    else:
        oper = "="

    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            query += f" AND i_item_id {oper} ?"
            res.append(filter_attributes.item_id)
        if filter_attributes.product_name is not None:
            query+= f" AND i_product_name {oper} ?"
            res.append(filter_attributes.product_name)
        if filter_attributes.category is not None:
            query+= f" AND i_category {oper} ?"
            res.append(filter_attributes.category)

        if filter_attributes.brand is not None:
            query += f" AND i_brand {oper} ?"
            res.append(filter_attributes.brand)
        
        if filter_attributes.manufact is not None:
            query+= f" AND i_manufact {oper} ?"
            res.append(filter_attributes.manufact)
        if filter_attributes.current_price is not None and filter_attributes.current_price != -1:
            query += " AND i_current_price = ?"
            res.append(filter_attributes.current_price)

    if max_price != -1:
        query += " AND i_current_price <= ?"
        res.append(max_price)

    if min_price != -1:
        query+= " AND i_current_price >= ?"
        res.append(min_price)

    if min_start_year != -1:
        query += " AND YEAR(i_rec_start_date) >= ?"
        res.append(min_start_year)

    if max_start_year != -1:
        query += " AND YEAR(i_rec_start_date) <= ?"
        res.append(max_start_year)

    cur.execute(query, res)
    rows = cur.fetchall()
    answers = []
    for r in rows:
        item = Item(
            item_id=r[0].strip(),
            product_name=r[1].strip(),
            brand=r[2].strip(),
            category=r[3].strip(),
            manufact=r[4].strip(),
            current_price=float(r[5]),
            start_year=int(r[6]),
            num_owned=int(r[7])
        )
        answers.append(item)
    return answers

def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    # You are returning a list of customers matching the given filters
    query = """
        SELECT c.c_customer_id, c.c_first_name, c.c_last_name,
               c.c_email_address, ca.ca_street_number, ca.ca_street_name,
               ca.ca_city, ca.ca_state, ca.ca_zip
        FROM customer c
        JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
        WHERE 1=1
    """
    res = []
    if use_patterns == True:
        oper = "LIKE"
    else:
        oper = "="

    if filter_attributes is not None:
        if filter_attributes.customer_id is not None:
            query+= " AND c.c_customer_id " + oper + " ?"
            res.append(filter_attributes.customer_id)
        if filter_attributes.name is not None:
            query += " AND c.c_first_name " + oper + " ?"
            res.append(filter_attributes.name)
        if filter_attributes.email is not None:
            query+= " AND c.c_email_address " + oper + " ?"
            res.append(filter_attributes.email)
        if filter_attributes.address is not None:
            query += " AND ca.ca_street_name " + oper + " ?"
            res.append(filter_attributes.address)

    cur.execute(query, res)
    rows = cur.fetchall()
    answers = []
    for r in rows:
        customer = Customer(
            customer_id=r[0].strip(),
            name=r[1].strip() + " " + r[2].strip(),
            email=r[3].strip(),
            address=r[4].strip() + " " + r[5].strip() + ", " + r[6].strip() + ", " + r[7].strip() + " " + r[8].strip()
        )
        answers.append(customer)
    return answers


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    # You are returning a list of active rentals matching the given filters
    query = "SELECT item_id, customer_id, rental_date, due_date FROM rental WHERE 1=1"
    res = []
 
    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            query += " AND item_id = ?"
            res.append(filter_attributes.item_id)
        if filter_attributes.customer_id is not None:
            query += " AND customer_id = ?"
            res.append(filter_attributes.customer_id)
        
    if max_rental_date is not None:
        query+= " AND rental_date <= ?"
        res.append(max_rental_date)
    if min_rental_date is not None:
        query+= " AND rental_date >= ?"
        res.append(min_rental_date)
    
    if min_due_date is not None:
        query += " AND due_date >= ?"
        res.append(min_due_date)
    if max_due_date is not None:
        query+= " AND due_date <= ?"
        res.append(max_due_date)
 
    cur.execute(query, res)
    rows = cur.fetchall()
    answers = []
    for r in rows:
        rental = Rental(
            item_id=r[0].strip(),
            customer_id=r[1].strip(),
            rental_date=str(r[2]),
            due_date=str(r[3])
        )
        answers.append(rental)
    return answers


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
    # You are returning a list of completed rentals matching the given filters
    query = "SELECT item_id, customer_id, rental_date, due_date, return_date FROM rental_history WHERE 1=1"
    res = []


    
    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        res.append(filter_attributes.customer_id)
    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        res.append(filter_attributes.item_id)

    if min_rental_date:
        query += " AND rental_date >= ?"
        res.append(min_rental_date)
    if max_rental_date:
        query += " AND rental_date <= ?"
        res.append(max_rental_date)
    
    if max_due_date:
        query+= " AND due_date <= ?"
        res.append(max_due_date)
    
    if min_return_date:
        query+= " AND return_date >= ?"
        res.append(min_return_date)

    if min_due_date:
        query+= " AND due_date >= ?"
        res.append(min_due_date)

    if max_return_date:
        query += " AND return_date <= ?"
        res.append(max_return_date)

    cur.execute(query, res)
    rows = cur.fetchall()
    answers = []
    for r in rows:
        rentalHistory = RentalHistory(
            item_id=r[0].strip(),
            customer_id=r[1].strip(),
            rental_date=str(r[2]),
            due_date=str(r[3]),
            return_date=str(r[4])
        )
        answers.append(rentalHistory)
    return answers


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    # You are returning a list of waitlist entries matching the given filters
    query = "SELECT item_id, customer_id, place_in_line FROM waitlist WHERE 1=1"
    res = []

    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        res.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        res.append(filter_attributes.customer_id)
        

    if min_place_in_line != -1:
        query += " AND place_in_line >= ?"
        res.append(min_place_in_line)
    if max_place_in_line != -1:
        query += " AND place_in_line <= ?"
        res.append(max_place_in_line)

    cur.execute(query, res)
    rows = cur.fetchall()
    answers = []
    for r in rows:
        waitList = Waitlist(
            item_id=r[0].strip(),
            customer_id=r[1].strip(),
            place_in_line=int(r[2])
        )
        answers.append(waitList)
    return answers


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
Returns how many peoperle are on the waitlist for this item.
"""
def line_length(item_id: str = None) -> int:
    # Get Number Of Peoperle On Waitlist
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