

class BillItem(dobject):
	item_no = datt(str)
	name    = datt(str)
	qty     = datt(Decimal)

	dident(item_no)

class Bill(dobject):
	bill_no = datt(str)
	items   = dset(BillItem)
	notes   = datt(str)

	dident(bill_no)
