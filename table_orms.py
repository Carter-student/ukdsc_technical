from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Customers(Base):
    __tablename__ = 'customers'

    customer_id = Column(Integer, primary_key=True, unique=True, nullable=False)
    customer_name = Column(String, nullable=False) 
    age = Column(Integer, nullable=False) 
    region = Column(String, nullable=False) 

    def __repr__(self):
        return f"<customers(customer_id={self.customer_id}, customer_name='{self.customer_name}', age={self.age}, region='{self.region}')>"

class Sales(Base):
    __tablename__ = 'sales'

    sale_id = Column(Integer, primary_key=True, unique=True, nullable=False)
    customer_id = Column(Integer, ForeignKey('customers.customer_id'), nullable=False)
    sale_date = Column(Date, nullable=False) 
    product_category = Column(String, nullable=False) 
    sale_amount = Column(Float, nullable=False)

    def __repr__(self):
        return (f"<sales(sale_id={self.sale_id}, customer_id={self.customer_id}, "
                f"sale_date={self.sale_date}, product_category='{self.product_category}', "
                f"sale_amount={self.sale_amount})>")